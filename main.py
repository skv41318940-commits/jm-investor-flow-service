"""
cloud_investor_flow_service/main.py
──────────────────────────────────────────────────────
종목별 수급현황(opt10059/pykrx) 동기화를 PC 없이 클라우드에서 상시 실행하는 서비스.

pykrx는 키움 OpenAPI와 무관하게 KRX 공식 데이터를 인터넷으로 가져오는
라이브러리라서, 이 서비스는 영웅문/키움 로그인 없이도 어디서든 동작합니다.

기존 investor_flow_fetch.py(PC용)와 로직은 동일하고, 실행 위치만 바뀐 것뿐입니다.
프론트엔드(FundFlowChart.tsx)는 코드 변경 없이 이 서비스 URL만 바라보면 됩니다.

배포 방법: 이 폴더 아래 README_배포방법.md 참고 (Render 무료 티어 기준)
"""
import os
import time
import json
from datetime import datetime, timedelta, timezone

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ── Render 서버는 UTC로 돌아가서, now_kst()를 그냥 쓰면 한국시간보다 9시간
# 느리게 나옴 (특히 자정 근처엔 날짜까지 하루 밀림). 반드시 이 함수로만 "지금"을 구함.
KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    return datetime.now(KST)


# ⚠️ pykrx는 import되는 순간 내부적으로 KRX 웹사이트에 자동 로그인을 시도함
# (KRX_ID/KRX_PW 환경변수 사용). KRX 서버가 그 순간 잠깐이라도 이상한 응답(빈 응답,
# 오류 페이지 등)을 주면 pykrx가 예외 처리 없이 그대로 죽는데, 이게 import 시점에
# 터지면 uvicorn이 앱을 아예 못 띄우고 서비스 전체가 죽어버림 (2026-08-06 저녁에 실제
# 발생한 장애). 이런 일시적 KRX 쪽 문제로 서비스 전체가 죽지 않도록 몇 번 재시도함.
_PYKRX_IMPORT_RETRIES = 5
for _pykrx_attempt in range(1, _PYKRX_IMPORT_RETRIES + 1):
    try:
        from pykrx import stock

        break
    except Exception as _pykrx_err:
        print(f"[pykrx import] {_pykrx_attempt}/{_PYKRX_IMPORT_RETRIES}번째 시도 실패: {_pykrx_err}")
        if _pykrx_attempt == _PYKRX_IMPORT_RETRIES:
            raise
        time.sleep(5)

from supabase import create_client

# ── 환경변수로 받음 (Render 대시보드에서 설정) ──
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # RLS 우회해서 쓰기 위해 service role 사용

# ── 한국투자증권(KIS) API — 회원사별/프로그램매매 등 pykrx엔 없는 데이터용 ──
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
KIS_CANO = os.environ.get("KIS_CANO")
KIS_ACNT_PRDT_CD = os.environ.get("KIS_ACNT_PRDT_CD", "01")
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"

# ── FRED (미국 연방준비제도 공식 경제데이터) — 美 국채금리 등 ──
FRED_API_KEY = os.environ.get("FRED_API_KEY")

# ── KRX 자체 Open API (openapi.krx.co.kr) — 코스피200 선물 등 파생상품 ──
KRX_AUTH_KEY = os.environ.get("KRX_AUTH_KEY")

# ── FMP (Financial Modeling Prep) — 니켈 등 원자재 ──
FMP_API_KEY = os.environ.get("FMP_API_KEY")

app = FastAPI(title="JM Investor Flow Cloud Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 필요하면 본인 vercel 도메인만 허용하도록 좁혀도 됨
    allow_methods=["GET"],
    allow_headers=["*"],
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── KIS 접근 토큰 캐시 (하루 1회 발급이 원칙이라 메모리에 캐싱해서 재사용) ──
_kis_token_cache = {"token": None, "expires_at": 0}


def get_kis_token() -> str:
    now = time.time()
    if _kis_token_cache["token"] and now < _kis_token_cache["expires_at"] - 300:
        return _kis_token_cache["token"]

    res = requests.post(
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={
            "grant_type": "client_credentials",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
        },
        timeout=10,
    )
    res.raise_for_status()
    data = res.json()
    token = data["access_token"]
    expires_in = int(data.get("expires_in", 86400))
    _kis_token_cache["token"] = token
    _kis_token_cache["expires_at"] = now + expires_in
    return token


def kis_get(path: str, tr_id: str, params: dict) -> dict:
    token = get_kis_token()
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": tr_id,
        "custtype": "P",
    }
    res = requests.get(f"{KIS_BASE_URL}{path}", headers=headers, params=params, timeout=10)
    res.raise_for_status()
    return res.json()


# ══════════════════════════════════════════════════════════════════════════
# 실시간 웹소켓 체결 누적기 — 정밀 세력평단용
# ──────────────────────────────────────────────────────────────────────────
# 근사치(양봉/음봉 추정) 대신, 실제 매수/매도 체결 구분(CCLD_DVSN)을 웹소켓으로
# 계속 받아서 분 단위로 누적함. Render 유료(상시 실행) 플랜에서만 의미 있음
# (무료 플랜은 잠들어서 연결이 계속 끊김).
# ══════════════════════════════════════════════════════════════════════════
import asyncio
import threading

try:
    import websockets
except ImportError:
    websockets = None

KIS_WS_URL = "ws://ops.koreainvestment.com:21000"

# 넥스트레이드(NXT) 거래가능 종목 전체 목록 (2026-07-27 기준, 608종목) —
# 사용자가 nextrade.co.kr에서 직접 확인해서 준 목록. "A" 접두어 뗀 순수 종목코드.
# 이 목록은 NXT가 종목을 추가/제외할 때마다 바뀔 수 있어서 주기적으로 갱신 필요.
NXT_ELIGIBLE_CODES = [
    "000660", "005930", "035420", "009150", "402340", "017670", "042660", "010120", "005380", "018260",
    "042700", "034020", "066570", "277810", "010060", "460930", "011070", "012330", "108860", "064350",
    "484810", "006400", "012450", "207940", "096770", "399720", "010140", "196170", "336260", "119850",
    "316140", "058610", "079550", "095610", "068270", "000500", "087010", "214450", "034730", "000270",
    "298040", "086520", "486990", "272210", "278470", "352820", "000250", "005490", "307950", "082740",
    "047810", "006800", "329180", "483650", "319660", "028260", "003230", "108490", "011200", "267260",
    "007660", "304100", "028300", "222800", "062040", "101490", "237690", "010950", "448900", "015760",
    "373220", "078930", "000150", "105560", "089970", "073240", "439090", "300080", "257720", "000720",
    "200710", "402030", "016360", "009540", "086790", "055550", "267270", "090430", "000990", "047050",
    "006260", "000100", "199430", "176750", "138040", "247540", "103590", "086280", "0126Z0", "058470",
    "051910", "192820", "066970", "161890", "437730", "071970", "141080", "181710", "005090", "377300",
    "004170", "003670", "090360", "005290", "071050", "011790", "095340", "298380", "281820", "466100",
    "032830", "031980", "083450", "226950", "488280", "039490", "131970", "131290", "007390", "007810",
    "024110", "047920", "033100", "099190", "009420", "270660", "084370", "263750", "128940", "340570",
    "489790", "089890", "195870", "323280", "065350", "161000", "052690", "290650", "323410", "003160",
    "017960", "267250", "039030", "218410", "004020", "476830", "171090", "007690", "102940", "456040",
    "005940", "310210", "000880", "083650", "051900", "035900", "006110", "022100", "078600", "251970",
    "030200", "033780", "064820", "183300", "020150", "036570", "356860", "419530", "075580", "180640",
    "010130", "041510", "161390", "103140", "175330", "126340", "077970", "484870", "004370", "145020",
    "420770", "069960", "166090", "001450", "458870", "000810", "476060", "003550", "229640", "008930",
    "259960", "096530", "018670", "457190", "030520", "082920", "298020", "037710", "049070", "124500",
    "034230", "100840", "326030", "039440", "195940", "058970", "005070", "032350", "008770", "029460",
    "241560", "214150", "045100", "060370", "026960", "002020", "357780", "039200", "002380", "241710",
    "439260", "443060", "074600", "001040", "090460", "017800", "456160", "023530", "475830", "120110",
    "094170", "101160", "044490", "388720", "107640", "011210", "328130", "089860", "460860", "397030",
    "139480", "064760", "051600", "086390", "068760", "102710", "097950", "295310", "041830", "114810",
    "000120", "059090", "032640", "117730", "445680", "294870", "005830", "099320", "137400", "032500",
    "036810", "086450", "112040", "348370", "168360", "445090", "021240", "388210", "287840", "001430",
    "089010", "389650", "0009K0", "042520", "023160", "455900", "185750", "011170", "029780", "108320",
    "018290", "127120", "033640", "139130", "161580", "025320", "079900", "036460", "348210", "138930",
    "122870", "213420", "014680", "036200", "382900", "098070", "302440", "189300", "030000", "140860",
    "272290", "007340", "459510", "383220", "046890", "001120", "020560", "298050", "000640", "110990",
    "194480", "002790", "005850", "082270", "003690", "095500", "053800", "011780", "468530", "036620",
    "009520", "035250", "214430", "042000", "079940", "214370", "000210", "085660", "271560", "452430",
    "013030", "358570", "005180", "014620", "033500", "006280", "093320", "003540", "033240", "007070",
    "023590", "009240", "004490", "094360", "441270", "475400", "389470", "052400", "036890", "475960",
    "001800", "101730", "462870", "424870", "499790", "069620", "394280", "304360", "006650", "061090",
    "001060", "190510", "092460", "121600", "251270", "361610", "078350", "015750", "108670", "004980",
    "053610", "282330", "006120", "078520", "336370", "004000", "111770", "403850", "115180", "005440",
    "249420", "004990", "067160", "199800", "005810", "009450", "365340", "004800", "0008Z0", "056190",
    "248070", "114190", "104830", "225570", "012630", "084110", "035760", "000080", "178920", "002810",
    "285130", "067080", "005420", "488900", "002960", "008060", "007310", "376300", "425420", "012750",
    "114090", "078340", "032190", "265520", "010780", "003570", "372320", "241770", "280360", "306200",
    "206640", "001500", "086900", "228760", "394800", "008490", "236200", "383800", "002710", "237880",
    "009900", "194700", "194370", "378340", "039130", "031210", "469610", "064960", "014830", "035600",
    "300720", "092730", "005300", "017940", "019170", "278280", "006040", "003240", "138610", "001720",
    "317450", "001680", "071320", "253450", "030610", "145720", "078160", "005500", "009970", "000240",
    "126720", "072710", "053030", "003030", "170900", "192080", "108380", "036530", "084850", "008730",
    "0120G0", "089980", "097520", "024720", "317330", "200670", "011760", "376270", "025540", "494120",
    "338220", "473980", "211050", "084690", "215200", "344820", "114840", "036830", "243070", "200880",
    "020000", "081660", "095660", "025900", "005250", "017810", "069080", "064550", "036800", "200130",
    "016610", "484120", "220100", "014820", "003850", "101360", "215000", "069260", "093520", "214320",
    "003200", "481070", "416180", "003220", "030190", "226320", "001130", "031430", "338840", "192400",
    "003090", "381970", "017390", "001750", "294570", "308430", "084010", "065680", "271940", "065660",
    "026890", "472850", "286940", "137310", "035150", "284740", "475560", "004690", "005610", "054950",
    "352480", "004360", "060980", "000070", "450950", "000670", "001530", "015360", "007160", "453340",
    "105630", "299030", "001940", "144510", "002240", "268280", "016590", "001270", "314930", "043150",
    "039840", "034950", "183190", "034310", "003960", "033270", "101930", "034120", "372170", "003300",
    "267980", "003920", "007700", "002030", "002840", "000320", "003120", "013890", "104700", "448280",
    "009680", "403550", "051500", "092230", "002320", "018310", "005710", "029530", "093050", "001630",
    "043370", "145990", "001460", "004700", "357550", "016800", "376900", "051360",
]


# ⚠️ 한투 실시간 등록 한도는 "실시간체결가+호가+예상체결+체결통보 합산 41건" —
# (TR ID, 종목코드) 조합 하나하나를 1건으로 셈. 지금 코드는 종목 1개당
# H0STCNT0(KRX)+H0NXCNT0(NXT) 2건을 쓰므로, 실제 최대 종목 수는 41 ÷ 2 = 20개.
# 이 숫자를 넘기면 MAX SUBSCRIBE OVER로 웹소켓 연결이 계속 끊기니 절대 건드리지 말 것.
NXT_WS_SUBSCRIBE_LIMIT = 20

# NXT 608종목 스캔 결과를 캐시에 몇 개까지 저장해둘지 — 상한(20)보다 넉넉하게 잡아둬야
# 상위권이 이미 일반 관심종목/최근추적과 겹쳐도 다음 순위로 자리를 채울 여지가 생김
NXT_RANKING_POOL_SIZE = 30

# NXT 608종목 자동 스캔을 하루 1번만 돌리는 기준 시각 (정규장 마감 직후, KST)
NXT_SCAN_HOUR = 15
NXT_SCAN_MINUTE = 35

_tick_buckets: dict = {}  # (code, market, trade_date, minute) -> {"buy_qty","buy_value","sell_qty","sell_value"}
_tick_lock = threading.Lock()
_ws_debug_count = 0  # 처음 몇 개 메시지만 원본 로그 남겨서 필드 위치 검증용

# ── 해외주식 정밀 세력평단 (HDFSCNT0, 2026-08-06 필드 분석으로 확정) ──────────────
# 필드는 "^"로 구분된 26개, 0-index 기준:
#   0 RSYM(실시간종목코드) 1 SYMB(종목코드) 2 ZDIV 3 TYMD(현지영업일자) 4 XYMD(현지일자)
#   5 XHMS(현지시간) 6 KYMD(한국일자) 7 KHMS(한국시간) 8 OPEN 9 HIGH 10 LOW 11 LAST(현재가)
#   12 SIGN 13 DIFF 14 RATE 15 PBID 16 PASK 17~19 (호가잔량 등)
#   20 누적거래량(TVOL) 21 누적거래대금(TAMT)
#   22 누적 매도체결량  23 누적 매수체결량  ← 실제 데이터로 직접 검증 완료 (체결강도=23/22*100 일치)
#   24 체결강도 25 (고정값, 미상)
# 한투가 이미 "누적" 값을 주기 때문에, 국내(H0STCNT0)처럼 매수/매도를 직접 분류할 필요 없이
# 직전에 본 누적값과의 차이(delta)만 계산하면 이번 체결이 매수/매도 중 어느 쪽인지, 얼마나
# 체결됐는지 바로 알 수 있음.
OVERSEAS_FIELDS = 26
IDX_OS_SYMB = 1
IDX_OS_TYMD = 3  # 현지영업일자 — 이걸 trade_date로 씀 (한국일자 아님, 미국 거래일 기준)
IDX_OS_LAST = 11  # 현재가
IDX_OS_CUM_SELL = 22  # 누적 매도체결량
IDX_OS_CUM_BUY = 23  # 누적 매수체결량

# 해외주식은 608종목 스캔 같은 자동선별 없이, 사용자가 등록한 관심종목만 정밀 추적함.
# 국내처럼 41건 등록 한도가 있는지 해외 쪽은 아직 확인 안 됐어서, 일단 국내와 별개로
# 넉넉하지 않게 20개로 제한해둠 (문제 생기면 낮추기).
OVERSEAS_WS_SUBSCRIBE_LIMIT = 20

# 한투가 지원하는 해외 거래소 전체 코드 (해외주식 실시간체결가 HDFSCNT0 기준)
OVERSEAS_MARKETS = ("NAS", "NYS", "AMS", "TSE", "HKS", "SHS", "SZS", "HSX", "HNX")

# 프론트가 "NASDAQ"/"NYSE"/"AMEX" 같은 긴 이름을 보내는데,
# 한투 API(tr_key, HDFSCNT0)는 짧은 코드(NAS/NYS/AMS 등)를 기대함 — 여기서 흡수해서 어느 쪽
# 형식이 와도 항상 짧은 코드로 통일함 (2026-08-07 새벽에 이거 안 맞아서 구독 자체가
# 잘못된 tr_key로 나갔던 사고 재발 방지).
_OVERSEAS_MARKET_ALIASES = {
    "NASDAQ": "NAS", "NAS": "NAS",
    "NYSE": "NYS", "NYS": "NYS",
    "AMEX": "AMS", "AMS": "AMS",
    "TOKYO": "TSE", "TSE": "TSE",
    "HONGKONG": "HKS", "HONG KONG": "HKS", "HKS": "HKS",
    "SHANGHAI": "SHS", "SHS": "SHS",
    "SHENZHEN": "SZS", "SZS": "SZS",
    "HOCHIMINH": "HSX", "HO CHI MINH": "HSX", "HSX": "HSX",
    "HANOI": "HNX", "HNX": "HNX",
}


def _normalize_overseas_market(market: str) -> str:
    return _OVERSEAS_MARKET_ALIASES.get((market or "").upper(), (market or "").upper())

# 미국 주요 거래소 정규장 시간을 한국시간(KST) 기준으로 넉넉하게 잡은 창(자정을 넘어감).
# 서머타임에 따라 1시간씩 밀리는데, 정확한 계산 대신 좀 여유있게 잡아서 놓치는 것보단
# 낫게 함 (프리마켓~애프터마켓 포함해서 대략 이 시간대에 체결이 있음).
# ⚠️ 미국 거래소만 생각하고 21:00~07:00(KST) 창 하나만 뒀었는데, 도쿄/홍콩/상해/심천/
# 호치민/하노이는 한국 낮 시간대(대략 08:00~16:00 KST)에 열려서 이 창 밖이라 웹소켓이
# 아예 안 깨어있었음 (2026-08-07 285A 추적 안 되던 사고). 그래서 "이 시간대엔 어차피
# 아무 거래소도 안 열림"이라고 확신할 수 있는 구간(16:00~21:00 KST)만 쉬고, 나머진 항상
# 깨어있게 바꿈 — 아시아 낮 시간대 + 미국 저녁/새벽 시간대를 합치면 거의 하루 종일임.
OVERSEAS_WS_IDLE_START_HOUR = 16  # 16:00 KST부터
OVERSEAS_WS_IDLE_END_HOUR = 21  # 21:00 KST까지는 쉼 (그 사이엔 지원 거래소 전부 휴장)

# (symbol, market, trade_date) -> {"buy_qty","buy_value","sell_qty","sell_value","last_price"}
_overseas_tick_buckets: dict = {}
_overseas_tick_lock = threading.Lock()
# (symbol, market, trade_date) -> (직전에 본 누적매도량, 직전에 본 누적매수량) — delta 계산용
_overseas_last_cum: dict = {}


# 문서에 나열된 실시간체결가 필드 순서 (^로 구분된 인덱스) — 여기가 틀리면 전부 틀어지니
# 초반엔 _ws_debug_count로 원본을 찍어서 실제로 맞는지 꼭 확인할 것
IDX_CODE = 0
IDX_HOUR = 1
IDX_PRICE = 2
IDX_CNTG_VOL = 12
IDX_CCLD_DVSN = 21
IDX_BSOP_DATE = 33
TICK_FIELD_COUNT = 46


_approval_key_cache: dict = {}  # purpose -> {"key": ..., "issued_at": ...}
_approval_key_lock = threading.Lock()


def invalidate_ws_approval_key(purpose: str):
    """
    연결이 계속 실패하면(정확한 만료 시간을 몰라도) "이 키가 상했다"고 보고 캐시를 지워서,
    다음 get_ws_approval_key() 호출 때 강제로 새 키를 발급받게 함.
    """
    with _approval_key_lock:
        _approval_key_cache.pop(purpose, None)


def get_ws_approval_key(purpose: str = "domestic") -> str:
    """
    실시간 웹소켓용 승인키 발급.
    ⚠️ 한투 실시간 웹소켓은 승인키 하나당 접속을 하나만 허용하는 것으로 보임 — 국내/해외
    워커가 승인키를 "공유"해서 캐싱했더니, 국내가 이미 그 키로 접속해있는 상태에서 해외가
    같은 키로 또 접속을 시도하면서 계속 튕겨나가는 장애가 있었음(2026-08-07). 그래서
    purpose(domestic/overseas)별로 완전히 별도의 승인키를 발급·캐싱하도록 분리함.
    (발급 API 자체를 너무 자주 부르면 제한 걸리는 문제는 여전히 있어서, 각 purpose 안에서는
    6시간 캐싱 유지)
    """
    with _approval_key_lock:
        cached = _approval_key_cache.get(purpose)
        if cached and (time.time() - cached["issued_at"]) < 6 * 3600:
            return cached["key"]

        res = requests.post(
            f"{KIS_BASE_URL}/oauth2/Approval",
            json={"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "secretkey": KIS_APP_SECRET},
            timeout=10,
        )
        res.raise_for_status()
        key = res.json()["approval_key"]
        _approval_key_cache[purpose] = {"key": key, "issued_at": time.time()}
        return key


def _add_tick(code: str, market: str, hhmmss: str, bsop_date: str, price: float, qty: float, ccld_dvsn: str):
    if price <= 0 or qty <= 0:
        return
    if len(hhmmss) < 4:
        return
    minute = f"{hhmmss[:2]}:{hhmmss[2:4]}"
    if len(bsop_date) == 8:
        trade_date = f"{bsop_date[:4]}-{bsop_date[4:6]}-{bsop_date[6:8]}"
    else:
        trade_date = now_kst().strftime("%Y-%m-%d")

    key = (code, market, trade_date, minute)
    with _tick_lock:
        b = _tick_buckets.setdefault(
            key, {"buy_qty": 0.0, "buy_value": 0.0, "sell_qty": 0.0, "sell_value": 0.0, "last_price": None}
        )
        b["last_price"] = price  # 매수/매도 구분과 무관하게 이 분(minute)의 마지막 체결가로 계속 덮어씀
        if ccld_dvsn == "1":  # 매수
            b["buy_qty"] += qty
            b["buy_value"] += price * qty
        elif ccld_dvsn == "5":  # 매도
            b["sell_qty"] += qty
            b["sell_value"] += price * qty
        # "3"(장전 단일가 등)은 매수/매도 구분이 없어서 수량 누적은 스킵 (가격은 위에서 이미 반영)


def _parse_realtime_message(raw: str):
    global _ws_debug_count
    parts = raw.split("|")
    if len(parts) < 4:
        return
    encrypted_flag, tr_id, count_str, body = parts[0], parts[1], parts[2], parts[3]
    if encrypted_flag != "0":
        return  # 체결가는 보통 비암호화라 "0"만 처리 (혹시 "1"이 오면 이번 버전에선 무시)

    market = "KRX" if tr_id == "H0STCNT0" else "NXT" if tr_id == "H0NXCNT0" else None
    if not market:
        return

    try:
        count = int(count_str)
    except ValueError:
        count = 1

    fields = body.split("^")

    if _ws_debug_count < 5:
        print(f"[ws_debug] tr={tr_id} count={count} first_tick_fields={fields[:TICK_FIELD_COUNT]}")
        _ws_debug_count += 1

    for i in range(max(count, 1)):
        start = i * TICK_FIELD_COUNT
        chunk = fields[start : start + TICK_FIELD_COUNT]
        if len(chunk) < IDX_CCLD_DVSN + 1:
            continue
        try:
            code = chunk[IDX_CODE]
            hhmmss = chunk[IDX_HOUR]
            price = float(chunk[IDX_PRICE])
            cntg_vol = float(chunk[IDX_CNTG_VOL])
            ccld_dvsn = chunk[IDX_CCLD_DVSN]
            bsop_date = chunk[IDX_BSOP_DATE] if len(chunk) > IDX_BSOP_DATE else now_kst().strftime("%Y%m%d")
        except (ValueError, IndexError):
            continue
        _add_tick(code, market, hhmmss, bsop_date, price, cntg_vol, ccld_dvsn)


def _flush_tick_buckets():
    """
    메모리에 쌓인 분 단위 누적치를 Supabase에 반영 (기존 값에 더하는 방식).
    ⚠️ 저장이 실패한 건(예: 테이블이 없거나 Supabase가 잠깐 불안정할 때) 그냥 버리지 않고
    메모리에 되돌려서 다음 주기에 다시 시도함 — 안 그러면 그 순간 체결 데이터가 영구 유실됨
    (2026-08-06 프리마켓 때 tick_minute_flow 테이블이 없어서 전부 유실됐던 사고 재발 방지).
    """
    with _tick_lock:
        if not _tick_buckets:
            return
        pending = dict(_tick_buckets)
        _tick_buckets.clear()

    failed = {}
    for (code, market, trade_date, minute), delta in pending.items():
        try:
            existing = (
                supabase.table("tick_minute_flow")
                .select("*")
                .eq("stock_code", code)
                .eq("market", market)
                .eq("trade_date", trade_date)
                .eq("minute", minute)
                .limit(1)
                .execute()
            )
            row = existing.data[0] if existing.data else None
            merged = {
                "stock_code": code,
                "market": market,
                "trade_date": trade_date,
                "minute": minute,
                "buy_qty": (row["buy_qty"] if row else 0) + delta["buy_qty"],
                "buy_value": (row["buy_value"] if row else 0) + delta["buy_value"],
                "sell_qty": (row["sell_qty"] if row else 0) + delta["sell_qty"],
                "sell_value": (row["sell_value"] if row else 0) + delta["sell_value"],
                # 이번 주기에 새로 들어온 체결이 있으면 그 마지막가로, 없으면 기존 값 유지
                "last_price": delta.get("last_price") if delta.get("last_price") is not None else (row["last_price"] if row else None),
            }
            supabase.table("tick_minute_flow").upsert(merged).execute()
        except Exception as e:
            print(f"[tick_flush] {code} {market} {minute} 저장 실패 (다음 주기에 재시도): {e}")
            failed[(code, market, trade_date, minute)] = delta

    if failed:
        with _tick_lock:
            for key, delta in failed.items():
                b = _tick_buckets.setdefault(
                    key, {"buy_qty": 0.0, "buy_value": 0.0, "sell_qty": 0.0, "sell_value": 0.0, "last_price": None}
                )
                b["buy_qty"] += delta["buy_qty"]
                b["buy_value"] += delta["buy_value"]
                b["sell_qty"] += delta["sell_qty"]
                b["sell_value"] += delta["sell_value"]
                if delta.get("last_price") is not None:
                    b["last_price"] = delta["last_price"]


def _add_overseas_tick(symbol: str, market: str, trade_date: str, price: float, cum_sell: float, cum_buy: float):
    """
    HDFSCNT0 한 틱 처리. 한투가 이미 "누적" 매수/매도체결량을 주기 때문에, 직전에 저장해둔
    누적값과 비교해서 이번에 새로 늘어난 만큼(delta)만 buy_qty/sell_qty에 더함.
    직전 값이 없으면(이 세션 첫 틱) delta 계산을 스킵하고 기준값만 저장 — 안 그러면 첫 틱에
    "누적값 전체"가 델타로 잡혀서 그날 하루치가 첫 틱에 몰빵되는 오류가 생김.
    """
    if price <= 0:
        return
    key = (symbol, market, trade_date)

    with _overseas_tick_lock:
        prev = _overseas_last_cum.get(key)
        _overseas_last_cum[key] = (cum_sell, cum_buy)
        if prev is None:
            return  # 기준값만 세팅하고 이번 틱은 델타 계산 안 함

        prev_sell, prev_buy = prev
        delta_sell = cum_sell - prev_sell
        delta_buy = cum_buy - prev_buy
        # 자정 넘어가면서 한투 쪽에서 누적을 리셋하는 경우 delta가 음수로 나올 수 있음 —
        # 그럴 땐 그냥 스킵 (다음 틱부터 새 기준으로 다시 정상 추적됨)
        if delta_sell < 0 or delta_buy < 0:
            return

        b = _overseas_tick_buckets.setdefault(
            key, {"buy_qty": 0.0, "buy_value": 0.0, "sell_qty": 0.0, "sell_value": 0.0, "last_price": None}
        )
        b["last_price"] = price
        if delta_buy > 0:
            b["buy_qty"] += delta_buy
            b["buy_value"] += price * delta_buy
        if delta_sell > 0:
            b["sell_qty"] += delta_sell
            b["sell_value"] += price * delta_sell


def _parse_overseas_realtime_message(raw: str):
    """HDFSCNT0(해외주식 실시간 지연 체결가) 원본 메시지 파싱 — 필드 26개, 위 상수 참고"""
    parts = raw.split("|", 3)
    if len(parts) < 4:
        return
    encrypted_flag, tr_id, count_str, body = parts
    if encrypted_flag != "0" or tr_id != "HDFSCNT0":
        return

    try:
        count = int(count_str)
    except ValueError:
        count = 1

    fields = body.split("^")
    n_complete = (len(fields) // OVERSEAS_FIELDS) * OVERSEAS_FIELDS
    fields = fields[:n_complete]  # 메시지가 중간에 잘려서 마지막 레코드가 불완전하면 버림

    for i in range(0, len(fields), OVERSEAS_FIELDS):
        chunk = fields[i : i + OVERSEAS_FIELDS]
        try:
            rsym = chunk[0]  # 예: "DNASAAPL" — D(지연) + NAS(시장) + AAPL(종목코드)
            symbol = chunk[IDX_OS_SYMB]
            tymd = chunk[IDX_OS_TYMD]
            price = float(chunk[IDX_OS_LAST])
            cum_sell = float(chunk[IDX_OS_CUM_SELL])
            cum_buy = float(chunk[IDX_OS_CUM_BUY])
        except (ValueError, IndexError):
            continue
        if len(tymd) != 8 or len(rsym) < 4:
            continue
        market = rsym[1:4]  # RSYM에서 시장코드만 추출 (NAS/NYS/AMS)
        trade_date = f"{tymd[:4]}-{tymd[4:6]}-{tymd[6:8]}"
        _add_overseas_tick(symbol, market, trade_date, price, cum_sell, cum_buy)


def _flush_overseas_tick_buckets():
    """
    메모리에 쌓인 해외주식 하루치 누적 매수/매도를 Supabase(overseas_daily_flow)에 반영.
    국내 _flush_tick_buckets와 동일하게, 저장 실패한 건 버리지 않고 다음 주기에 재시도함.
    """
    with _overseas_tick_lock:
        if not _overseas_tick_buckets:
            return
        pending = dict(_overseas_tick_buckets)
        _overseas_tick_buckets.clear()

    failed = {}
    for (symbol, market, trade_date), delta in pending.items():
        try:
            existing = (
                supabase.table("overseas_daily_flow")
                .select("*")
                .eq("symbol", symbol)
                .eq("market", market)
                .eq("trade_date", trade_date)
                .limit(1)
                .execute()
            )
            row = existing.data[0] if existing.data else None
            merged = {
                "symbol": symbol,
                "market": market,
                "trade_date": trade_date,
                "buy_qty": (row["buy_qty"] if row else 0) + delta["buy_qty"],
                "buy_value": (row["buy_value"] if row else 0) + delta["buy_value"],
                "sell_qty": (row["sell_qty"] if row else 0) + delta["sell_qty"],
                "sell_value": (row["sell_value"] if row else 0) + delta["sell_value"],
                "last_price": delta.get("last_price") if delta.get("last_price") is not None else (row["last_price"] if row else None),
            }
            supabase.table("overseas_daily_flow").upsert(merged).execute()
        except Exception as e:
            print(f"[overseas_tick_flush] {symbol} {market} {trade_date} 저장 실패 (다음 주기에 재시도): {e}")
            failed[(symbol, market, trade_date)] = delta

    if failed:
        with _overseas_tick_lock:
            for key, delta in failed.items():
                b = _overseas_tick_buckets.setdefault(
                    key, {"buy_qty": 0.0, "buy_value": 0.0, "sell_qty": 0.0, "sell_value": 0.0, "last_price": None}
                )
                b["buy_qty"] += delta["buy_qty"]
                b["buy_value"] += delta["buy_value"]
                b["sell_qty"] += delta["sell_qty"]
                b["sell_value"] += delta["sell_value"]
                if delta.get("last_price") is not None:
                    b["last_price"] = delta["last_price"]


def fetch_nxt_scan_scores(limit: int = NXT_RANKING_POOL_SIZE):
    """
    NXT 거래가능 608종목(NXT_ELIGIBLE_CODES) 전체를 당일 일봉 기준으로 스캔해서
    점수 = 거래대금(억원) × (1 + |등락률(%)| ÷ 10) 상위 종목을 추려냄.
    fetch_volume_top30()과 같은 패턴 — ALL 마켓을 한 번에 불러온 뒤 NXT 대상만 필터링해서
    608종목 각각을 따로 조회하지 않음 (API 호출 1번으로 끝).
    """
    ymd = _latest_trading_day()
    df = stock.get_market_ohlcv_by_ticker(ymd, market="ALL")
    print(f"[nxt_scan] date={ymd} rows={len(df)}")

    if df.empty:
        raise ValueError(f"KRX에서 {ymd} 시세 데이터를 가져오지 못했습니다.")

    eligible = set(NXT_ELIGIBLE_CODES)
    df = df[df.index.isin(eligible)]
    if df.empty:
        raise ValueError("NXT 대상종목과 매칭되는 시세 데이터가 없습니다. NXT_ELIGIBLE_CODES 목록이 오래됐을 수 있습니다.")

    scored = []
    for code, row in df.iterrows():
        trading_value_eok = round(float(row.get("거래대금", 0)) / 1e8, 1)  # 억원
        change_pct = float(row.get("등락률", 0))
        score = trading_value_eok * (1 + abs(change_pct) / 10)
        scored.append({"code": code, "trading_value": trading_value_eok, "change_pct": change_pct, "score": score})

    scored.sort(key=lambda r: r["score"], reverse=True)
    trade_date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"

    rows = []
    for i, r in enumerate(scored[:limit], start=1):
        try:
            name = stock.get_market_ticker_name(r["code"])
        except Exception:
            name = r["code"]
        rows.append(
            {
                "scan_date": trade_date,
                "rank": i,
                "stock_code": r["code"],
                "stock_name": name,
                "score": round(r["score"], 2),
                "trading_value": r["trading_value"],
                "change_pct": r["change_pct"],
            }
        )
    return rows


def _resolve_nxt_scan_trade_date() -> str:
    """
    지금 스캔하면 실제로 어느 거래일자(YYYY-MM-DD)로 기록될지 미리 계산.
    ⚠️ now_kst()의 달력상 "오늘"과 다를 수 있음 — 예를 들어 정규장 마감(15:30) 전에
    테스트하면 KRX에 아직 오늘자 일봉이 없어서 _latest_trading_day()가 어제로 잡힘.
    저장/삭제/dedupe 체크를 이 값 하나로 통일해서 "오늘 날짜"와 "실제 스캔된 날짜"가
    어긋나 중복 키 에러나 나거나 조회했을 때 텅 비어 보이는 문제를 막음.
    """
    ymd = _latest_trading_day()
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"


def _ensure_nxt_ranking_cached():
    """
    스캔 기준시각(15:35) 이후, 그날 거래일 캐시가 아직 없으면 딱 1번 스캔해서 채워둠.
    (기준시각 전이거나 주말이면 조용히 넘어감 — ws_worker가 5분마다 다시 확인)
    """
    now = now_kst()
    if now.weekday() >= 5:
        return
    scan_time = now.replace(hour=NXT_SCAN_HOUR, minute=NXT_SCAN_MINUTE, second=0, microsecond=0)
    if now < scan_time:
        return

    try:
        trade_date = _resolve_nxt_scan_trade_date()
    except Exception as e:
        print(f"[nxt_ranking] 거래일 확인 실패: {e}")
        return

    try:
        existing = (
            supabase.table("nxt_daily_ranking").select("stock_code").eq("scan_date", trade_date).limit(1).execute()
        )
        if existing.data:
            return
    except Exception as e:
        print(f"[nxt_ranking] 캐시 확인 실패: {e}")
        return

    try:
        rows = fetch_nxt_scan_scores(limit=NXT_RANKING_POOL_SIZE)
        supabase.table("nxt_daily_ranking").delete().eq("scan_date", trade_date).execute()
        supabase.table("nxt_daily_ranking").insert(rows).execute()
        print(f"[nxt_ranking] {trade_date} 스캔 완료, {len(rows)}종목 캐싱")
    except Exception as e:
        print(f"[nxt_ranking] 스캔 실패: {e}")


def _get_nxt_ranking_codes(limit: int) -> list:
    """
    가장 최근에 스캔된 NXT 순위에서 점수 상위 종목코드를 반환.
    ⚠️ "오늘 날짜"로 정확히 맞추지 않고 "가장 최근 scan_date"를 찾음 — 그래야 그날
    애프터마켓뿐 아니라 다음날 프리마켓(그 다음 스캔이 아직 안 돈 시점)에도 어제
    캐시를 그대로 재사용할 수 있음.
    """
    if limit <= 0:
        return []
    try:
        latest = (
            supabase.table("nxt_daily_ranking").select("scan_date").order("scan_date", desc=True).limit(1).execute()
        )
        if not latest.data:
            return []
        latest_date = latest.data[0]["scan_date"]
        res = (
            supabase.table("nxt_daily_ranking")
            .select("stock_code")
            .eq("scan_date", latest_date)
            .order("rank")
            .limit(limit)
            .execute()
        )
        return [r["stock_code"] for r in res.data]
    except Exception as e:
        print(f"[nxt_ranking] 조회 실패: {e}")
        return []



def _get_watchlist_codes() -> list:
    """
    웹소켓 구독 목록을 NXT_WS_SUBSCRIBE_LIMIT(20개) 상한 안에서 우선순위대로 채움:
      1) NXT 전용 관심종목 (최대 10개) — 무조건 포함
      2) 일반 관심종목(watchlist)
      3) 최근 14일 이내 조회한 종목(Control/종목분석 검색 이력) — 최근 조회일 순
      4) 그래도 남는 슬롯 — 오늘자 NXT 608종목 자동 스캔 점수 상위로 채움
         (당일 15:35 스캔 전이면 캐시가 없어서 이 단계는 빈 목록 → 3번까지만 채워짐)
    """
    codes: list = []
    seen = set()

    def add(new_codes):
        for c in new_codes:
            if c and c not in seen and len(codes) < NXT_WS_SUBSCRIBE_LIMIT:
                seen.add(c)
                codes.append(c)

    try:
        res0 = supabase.table("nxt_watchlist").select("stock_code").order("added_at").execute()
        add([r["stock_code"] for r in res0.data])
    except Exception as e:
        print(f"[nxt_watchlist] 조회 실패: {e}")

    try:
        res1 = supabase.table("watchlist").select("code").execute()
        add([r["code"] for r in res1.data])
    except Exception as e:
        print(f"[watchlist] 조회 실패: {e}")

    try:
        cutoff = (now_kst() - timedelta(days=14)).isoformat()
        res2 = (
            supabase.table("tick_tracked_codes")
            .select("stock_code")
            .gte("last_requested_at", cutoff)
            .order("last_requested_at", desc=True)
            .execute()
        )
        add([r["stock_code"] for r in res2.data])
    except Exception as e:
        print(f"[tick_tracked_codes] 조회 실패: {e}")

    # remaining 개수만 요청하면, 상위권이 이미 관심종목/최근추적과 겹칠 때 자리가
    # 남아도 다음 순위를 못 가져와서 슬롯이 낭비됨 — 캐시 전체(30개)를 넘겨서
    # add()가 중복 제외하며 남는 자리를 끝까지 채우게 함
    if len(codes) < NXT_WS_SUBSCRIBE_LIMIT:
        add(_get_nxt_ranking_codes(NXT_RANKING_POOL_SIZE))

    return codes


@app.get("/api/overseas-watchlist")
def overseas_watchlist_list():
    """해외주식 정밀 추적 관심종목 목록 조회"""
    try:
        res = supabase.table("overseas_watchlist").select("*").order("added_at").execute()
        return {"ok": True, "items": res.data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-watchlist/add")
def overseas_watchlist_add(symbol: str, name: str, market: str = "NAS"):
    """
    해외주식 관심종목 추가 — 최대 OVERSEAS_WS_SUBSCRIBE_LIMIT개까지만.
    market: NAS(나스닥)/NYS(뉴욕)/AMS(아멕스)/TSE(도쿄)/HKS(홍콩)/SHS(상해)/SZS(심천)/HSX(호치민)/HNX(하노이)
    """
    try:
        market = _normalize_overseas_market(market)
        if market not in OVERSEAS_MARKETS:
            return {"ok": False, "error": f"market은 {'/'.join(OVERSEAS_MARKETS)} 중 하나여야 해요."}
        existing = supabase.table("overseas_watchlist").select("symbol,market").execute()
        pairs = {(r["symbol"], r["market"]) for r in existing.data}
        if (symbol, market) in pairs:
            return {"ok": True, "message": "이미 등록되어 있습니다."}
        if len(pairs) >= OVERSEAS_WS_SUBSCRIBE_LIMIT:
            return {"ok": False, "error": f"해외주식 관심종목은 최대 {OVERSEAS_WS_SUBSCRIBE_LIMIT}개까지만 등록할 수 있습니다."}
        supabase.table("overseas_watchlist").insert({"symbol": symbol, "name": name, "market": market}).execute()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-watchlist/remove")
def overseas_watchlist_remove(symbol: str, market: str = "NAS"):
    """해외주식 관심종목 제거"""
    try:
        supabase.table("overseas_watchlist").delete().eq("symbol", symbol).eq("market", _normalize_overseas_market(market)).execute()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-subscribe-preview")
def overseas_subscribe_preview_endpoint():
    """
    지금 이 순간 실제로 웹소켓에 구독될 해외주식 목록(최대 OVERSEAS_WS_SUBSCRIBE_LIMIT개)을
    소스별(watchlist/tracked)로 보여줌 — 국내 /api/ws-subscribe-preview와 같은 용도.
    ⚠️ 한투 해외주식 실시간 등록 한도가 국내(41건)와 별개인지 공식 문서로 확인이 안 돼서,
    일단 보수적으로 20개로 잡아뒀음 — 이 엔드포인트로 지금 몇 개 쓰고 있는지 눈으로 보면서
    관리할 것.
    """
    try:
        pairs = _get_overseas_watchlist_symbols()
        try:
            watchlist_pairs = {
                (r["symbol"], _normalize_overseas_market(r["market"]))
                for r in supabase.table("overseas_watchlist").select("symbol,market").execute().data
            }
        except Exception:
            watchlist_pairs = set()
        items = [
            {"symbol": s, "market": m, "source": "overseas_watchlist" if (s, m) in watchlist_pairs else "overseas_tracked_codes"}
            for s, m in pairs
        ]
        return {"ok": True, "limit": OVERSEAS_WS_SUBSCRIBE_LIMIT, "count": len(items), "items": items}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-search")
def overseas_search_endpoint(q: str, limit: int = 8):
    """
    해외주식 종목 검색 — 두 테이블을 합쳐서 검색함:
      1) overseas_name_aliases_kr — 사람이 직접 큐레이션한 한글 별칭(종목명 한글→티커+거래소).
         한글 검색은 이 테이블에 있는 것만 됨(자동 번역 아님) — 매칭되면 거래소까지 바로 확정됨.
      2) overseas_stock_master — 나스닥 스크리너 7205종목 + 다우30 등, 티커/영문명 검색용.
    한글 별칭 결과를 먼저 붙이고, 그다음 영문/티커 검색 결과를 이어붙임 (중복 제거).
    """
    q = q.strip()
    if not q:
        return {"ok": True, "items": []}
    try:
        items = []
        seen = set()

        try:
            alias_res = (
                supabase.table("overseas_name_aliases_kr")
                .select("keyword_kr,symbol,market,name_en")
                .ilike("keyword_kr", f"%{q}%")
                .limit(limit)
                .execute()
            )
            for r in alias_res.data:
                if r["symbol"] not in seen:
                    seen.add(r["symbol"])
                    items.append({"symbol": r["symbol"], "name": r.get("name_en") or r["keyword_kr"], "market": r["market"]})
        except Exception as e:
            print(f"[overseas_search] 한글 별칭 검색 실패: {e}")

        remaining = limit - len(items)
        if remaining > 0:
            res = (
                supabase.table("overseas_stock_master")
                .select("symbol,name,market")
                .or_(f"symbol.ilike.%{q}%,name.ilike.%{q}%")
                .limit(remaining)
                .execute()
            )
            master_rows = list(res.data)

            # ⚠️ 나스닥 스크리너 CSV가 실제로는 나스닥이 아닌 다른 거래소 종목도 섞여있어서
            # (예: IONQ는 실제 NYSE인데 NAS로 잘못 들어있었음, 2026-08-08 확인됨),
            # 검증된 별칭 테이블에 같은 심볼이 있으면 그 거래소 정보로 덮어씀
            symbols = [r["symbol"] for r in master_rows]
            corrections = {}
            if symbols:
                try:
                    corr_res = (
                        supabase.table("overseas_name_aliases_kr").select("symbol,market").in_("symbol", symbols).execute()
                    )
                    corrections = {r["symbol"]: r["market"] for r in corr_res.data}
                except Exception as e:
                    print(f"[overseas_search] 거래소 정정 조회 실패: {e}")

            for r in master_rows:
                if r["symbol"] in corrections:
                    r["market"] = corrections[r["symbol"]]
                if r["symbol"] not in seen:
                    seen.add(r["symbol"])
                    items.append(r)

        return {"ok": True, "items": items}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-overseas-daily-debug")
def kis_overseas_daily_debug(symbol: str = "AAPL", market: str = "NAS"):
    """
    디버그 전용 — 해외주식 기간별시세(일봉, HHDFS76240000) 원본 응답을 그대로 반환.
    근사치(양봉/음봉 기반) 폴백을 만들기 전에 output2 필드명이 정확히 뭔지 확인하려는 용도.
    """
    try:
        data = kis_get(
            "/uapi/overseas-price/v1/quotations/dailyprice",
            "HHDFS76240000",
            {"AUTH": "", "EXCD": market, "SYMB": symbol, "GUBN": "0", "BYMD": now_kst().strftime("%Y%m%d"), "MODP": "0"},
        )
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-resolve")
def overseas_resolve_endpoint(symbol: str):
    """
    "통합 검색" 지원용 — 특정 종목코드가 9개 해외거래소 중 어디 상장인지 자동으로 찾아줌.
    한투에 해외주식 "종목명" 검색 API가 따로 없어서(마스터파일 다운로드 방식만 있음),
    대신 "해외주식 현재가"(HHDFS00000300)를 거래소별로 순서대로 호출해보고 실제 값이
    있는 곳을 찾는 방식 — 종목코드만 정확하면 국가/거래소를 몰라도 바로 조회 가능함.
    ⚠️ 아직 실제 응답 필드(특히 종목명)를 실전 데이터로 검증 안 해서, 이름이 안 나오면
    일단 종목코드를 그대로 이름으로 씀 — 다음에 실제 응답 보고 다듬을 것.
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return {"ok": False, "error": "종목코드를 입력해주세요."}

    for market in OVERSEAS_MARKETS:
        try:
            data = kis_get(
                "/uapi/overseas-price/v1/quotations/price",
                "HHDFS00000300",
                {"AUTH": "", "EXCD": market, "SYMB": symbol},
            )
            output = data.get("output") or {}
            last = output.get("last")
            if data.get("rt_cd") == "0" and last not in (None, "", "0", "0.00"):
                return {
                    "ok": True,
                    "symbol": symbol,
                    "market": market,
                    "name": output.get("name") or symbol,
                    "last": last,
                }
        except Exception:
            continue

    return {"ok": False, "error": f"'{symbol}' 종목을 9개 거래소 어디에서도 찾지 못했어요. 코드가 정확한지 확인해주세요."}


def _estimate_overseas_daily_avg(symbol: str, market: str, start: str, end: str) -> dict:
    """
    해외주식 근사치 세력평단 — 정밀 추적 데이터가 없을 때 쓰는 폴백.
    한투 해외주식 기간별시세(HHDFS76240000)의 일봉으로, 국내 근사치랑 같은 방식
    (시가 대비 종가 움직임 크기로 그날 거래량을 매수/매도로 추정 배분)으로 계산함.
    실제 매수/매도 체결 구분이 아니라 "추정"이라 항상 근사치.

    ⚠️ 이 API가 가끔 output2가 비어있는 등 일시적으로 이상한 응답을 줄 때가 있어서
    (2026-08-07~08 확인됨), 최대 3번 재시도하되 매번 BYMD(기준일자)를 다르게
    시도함 — 오늘 날짜, 어제 날짜, 빈 값(한투 기본 동작) 순서로 시도해서 뭐가
    먹히는지 확인. 특히 주말/휴장일엔 "오늘"이 거래일이 아니라서 빈 응답이 나오는
    걸로 의심됨.
    """
    now = now_kst()
    yesterday = now - timedelta(days=1)
    bymd_attempts = [now.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d"), ""]

    rows = []
    last_error = None
    for attempt, bymd in enumerate(bymd_attempts):
        try:
            data = kis_get(
                "/uapi/overseas-price/v1/quotations/dailyprice",
                "HHDFS76240000",
                {"AUTH": "", "EXCD": market, "SYMB": symbol, "GUBN": "0", "BYMD": bymd, "MODP": "0"},
            )
            rows = data.get("output2") or []
            if rows:
                break
            last_error = f"output2가 비어있음 (BYMD={bymd or '(빈값)'}, rt_cd={data.get('rt_cd')}, msg={data.get('msg1')})"
        except Exception as e:
            last_error = str(e)
        if attempt < len(bymd_attempts) - 1:
            time.sleep(1.5)
    if not rows:
        raise ValueError(f"해외 일봉 데이터를 가져오지 못했어요 ({len(bymd_attempts)}번 재시도 후에도 실패: {last_error}).")

    # 최신순으로 오니 오래된 순으로 뒤집고, 요청한 기간(start~end)만 필터링
    rows = sorted(rows, key=lambda r: r["xymd"])
    rows = [
        r
        for r in rows
        if start.replace("-", "") <= r["xymd"] <= end.replace("-", "")
    ]
    if not rows:
        raise ValueError(f"{start}~{end} 구간에 해당하는 일봉 데이터가 없어요.")

    candles = []
    cum_buy_qty = cum_buy_value = cum_sell_qty = cum_sell_value = 0.0
    for r in rows:
        try:
            open_p = float(r["open"])
            close_p = float(r["clos"])
            vol = float(r["tvol"])
        except (KeyError, ValueError):
            continue
        if open_p <= 0 or vol <= 0:
            continue

        if close_p >= open_p:
            ratio = min(((close_p - open_p) / open_p) * 5, 0.45)
            net_buy = vol * ratio
        else:
            ratio = min(((open_p - close_p) / open_p) * 5, 0.45)
            net_buy = -vol * ratio
        buy_qty = max(0.0, (vol + net_buy) / 2)
        sell_qty = max(0.0, (vol - net_buy) / 2)

        cum_buy_qty += buy_qty
        cum_buy_value += close_p * buy_qty
        cum_sell_qty += sell_qty
        cum_sell_value += close_p * sell_qty

        trade_date = f"{r['xymd'][:4]}-{r['xymd'][4:6]}-{r['xymd'][6:8]}"
        candles.append(
            {
                "date": trade_date,
                "close": close_p,
                "avg_price": round(cum_buy_value / cum_buy_qty, 2) if cum_buy_qty > 0 else 0,
                "sell_avg_price": round(cum_sell_value / cum_sell_qty, 2) if cum_sell_qty > 0 else 0,
            }
        )

    if not candles:
        raise ValueError("근사치를 계산할 유효한 일봉이 없어요.")

    return {
        "symbol": symbol,
        "market": market,
        "start": start,
        "end": end,
        "current_price": candles[-1]["close"],
        "force_buy_avg": round(cum_buy_value / cum_buy_qty, 2) if cum_buy_qty > 0 else 0,
        "force_sell_avg": round(cum_sell_value / cum_sell_qty, 2) if cum_sell_qty > 0 else 0,
        "buy_qty": round(cum_buy_qty),
        "sell_qty": round(cum_sell_qty),
        "candles": candles,
        "is_estimate": True,
    }


@app.get("/api/overseas-avg")
def overseas_avg_endpoint(symbol: str, market: str = "NAS", start: str = "", end: str = ""):
    """
    해외주식 기간별 세력평단 — overseas_daily_flow에 쌓인 일별 누적 매수/매도가 있으면
    정밀(is_estimate:false)로, 없으면 한투 해외 일봉 API 기반 근사치(is_estimate:true)로
    자동 폴백함. 국내(queryAvg)와 같은 2단 구조 — 관심종목/최근조회로 추적을 시작한
    기간만 정밀이고, 나머지는 항상 근사치로라도 값을 보여줌.
    """
    market = _normalize_overseas_market(market)
    end = end or now_kst().strftime("%Y-%m-%d")
    if not start:
        start_dt = now_kst() - timedelta(days=90)
        start = start_dt.strftime("%Y-%m-%d")

    try:
        res = (
            supabase.table("overseas_daily_flow")
            .select("*")
            .eq("symbol", symbol)
            .eq("market", market)
            .gte("trade_date", start)
            .lte("trade_date", end)
            .order("trade_date")
            .execute()
        )
        rows = res.data
        if not rows:
            # 정밀 데이터가 없으면 근사치로 자동 폴백 (국내와 동일한 2단 구조)
            try:
                fallback = _estimate_overseas_daily_avg(symbol, market, start, end)
                fallback["note"] = "이 종목을 검색하면 자동으로 추적이 시작되니, 다음 미국장부터는 정밀 데이터로 바뀌어요."
                return fallback
            except Exception as e:
                return {
                    "error": f"{start}~{end} 구간에 정밀 추적 데이터가 없고, 근사치 계산도 실패했어요: {e}"
                }

        buy_qty_total = sum(r["buy_qty"] for r in rows)
        buy_value_total = sum(r["buy_value"] for r in rows)
        sell_qty_total = sum(r["sell_qty"] for r in rows)
        sell_value_total = sum(r["sell_value"] for r in rows)

        avg = round(buy_value_total / buy_qty_total, 2) if buy_qty_total > 0 else 0
        sell_avg = round(sell_value_total / sell_qty_total, 2) if sell_qty_total > 0 else 0

        candles = []
        cum_buy_qty = cum_buy_value = cum_sell_qty = cum_sell_value = 0.0
        for r in rows:
            cum_buy_qty += r["buy_qty"]
            cum_buy_value += r["buy_value"]
            cum_sell_qty += r["sell_qty"]
            cum_sell_value += r["sell_value"]
            candles.append(
                {
                    "date": r["trade_date"],
                    "close": r.get("last_price"),
                    "avg_price": round(cum_buy_value / cum_buy_qty, 2) if cum_buy_qty > 0 else 0,
                    "sell_avg_price": round(cum_sell_value / cum_sell_qty, 2) if cum_sell_qty > 0 else 0,
                }
            )

        return {
            "symbol": symbol,
            "market": market,
            "start": start,
            "end": end,
            "current_price": rows[-1].get("last_price"),
            "force_buy_avg": avg,
            "force_sell_avg": sell_avg,
            "buy_qty": buy_qty_total,
            "sell_qty": sell_qty_total,
            "candles": candles,
            "is_estimate": False,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/nxt-watchlist")
def nxt_watchlist_list():
    """NXT 전용 관심종목(최대 10개) 목록 조회"""
    try:
        res = supabase.table("nxt_watchlist").select("*").order("added_at").execute()
        return {"ok": True, "items": res.data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/nxt-eligible")
def nxt_eligible_endpoint(code: str):
    """이 종목이 NXT 거래가능 608종목에 포함되는지 확인 (관심종목 추가 전 프론트에서 미리 검증할 때 사용)"""
    return {"ok": True, "eligible": code in NXT_ELIGIBLE_CODES}


@app.get("/api/nxt-watchlist/add")
def nxt_watchlist_add(code: str, name: str):
    """NXT 전용 관심종목 추가 — 최대 10개까지만, NXT 거래가능 608종목만 허용"""
    try:
        if code not in NXT_ELIGIBLE_CODES:
            return {
                "ok": False,
                "error": f"{name}({code})은(는) NXT에서 거래되지 않는 종목이라 정밀 세력평단을 쌓을 수 없어요.",
            }
        existing = supabase.table("nxt_watchlist").select("stock_code").execute()
        codes = {r["stock_code"] for r in existing.data}
        if code in codes:
            return {"ok": True, "message": "이미 등록되어 있습니다."}
        if len(codes) >= 10:
            return {"ok": False, "error": "NXT 관심종목은 최대 10개까지만 등록할 수 있습니다."}
        supabase.table("nxt_watchlist").insert({"stock_code": code, "stock_name": name}).execute()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/nxt-watchlist/remove")
def nxt_watchlist_remove(code: str):
    """NXT 전용 관심종목 제거"""
    try:
        supabase.table("nxt_watchlist").delete().eq("stock_code", code).execute()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-nxt-ranking")
def sync_nxt_ranking_endpoint(limit: int = NXT_RANKING_POOL_SIZE, force: bool = False):
    """
    NXT 608종목 자동 스캔을 지금 바로 실행 (디버그/수동 트리거용).
    평소엔 ws_worker가 15:35 이후 자동으로 1회만 실행하므로 이 엔드포인트를 직접 부를 일은
    거의 없지만, 그날 결과를 미리 확인하거나 재스캔하고 싶을 때 사용. force=true면
    해당 거래일 캐시가 있어도 덮어씀.
    ⚠️ 정규장 마감(15:30) 전에 테스트하면 KRX에 아직 오늘자 일봉이 없어서, scan_date가
    "오늘"이 아니라 "가장 최근 완결된 거래일"(보통 어제)로 찍힐 수 있음 — 정상 동작.
    """
    try:
        trade_date = _resolve_nxt_scan_trade_date()
        if not force:
            existing = (
                supabase.table("nxt_daily_ranking").select("stock_code").eq("scan_date", trade_date).limit(1).execute()
            )
            if existing.data:
                return {
                    "ok": True,
                    "skipped": True,
                    "scan_date": trade_date,
                    "message": "이 거래일 스캔이 이미 있습니다 (force=true로 재스캔 가능)",
                }

        rows = fetch_nxt_scan_scores(limit=limit)
        supabase.table("nxt_daily_ranking").delete().eq("scan_date", trade_date).execute()
        supabase.table("nxt_daily_ranking").insert(rows).execute()
        return {"ok": True, "synced": len(rows), "scan_date": trade_date}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/nxt-ranking")
def nxt_ranking_endpoint(date: str = ""):
    """
    NXT 스캔 순위 조회 — 프론트엔드 표시용.
    date를 명시하면 그 날짜만 정확히 조회(과거 복습용). 비워두면 "오늘 날짜"가 아니라
    "가장 최근에 스캔된 날짜"를 보여줌 — 안 그러면 그날 15:35 스캔 전엔 매번 텅 비어
    보이는 문제가 있었음(구독 로직은 이미 이렇게 되어 있었는데 이 조회용 API만 안 맞춰져 있었음).
    """
    try:
        if date:
            trade_date = date
        else:
            latest = (
                supabase.table("nxt_daily_ranking").select("scan_date").order("scan_date", desc=True).limit(1).execute()
            )
            trade_date = latest.data[0]["scan_date"] if latest.data else now_kst().strftime("%Y-%m-%d")

        res = supabase.table("nxt_daily_ranking").select("*").eq("scan_date", trade_date).order("rank").execute()
        return {"ok": True, "scan_date": trade_date, "items": res.data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get_watchlist_codes_with_source() -> list:
    """
    _get_watchlist_codes()와 완전히 같은 우선순위 로직이지만, 각 종목이 어느 소스에서
    왔는지(source)까지 같이 반환 — "왜 이 종목이 포함/제외됐는지" 확인용 디버그 전용.
    실제 ws_worker는 이 함수가 아니라 _get_watchlist_codes()를 씀 (로직은 100% 동일).
    """
    codes: list = []
    seen = set()

    def add(new_codes, source):
        for c in new_codes:
            if c and c not in seen and len(codes) < NXT_WS_SUBSCRIBE_LIMIT:
                seen.add(c)
                codes.append({"code": c, "source": source})

    try:
        res0 = supabase.table("nxt_watchlist").select("stock_code").order("added_at").execute()
        add([r["stock_code"] for r in res0.data], "nxt_watchlist")
    except Exception as e:
        print(f"[nxt_watchlist] 조회 실패: {e}")

    try:
        res1 = supabase.table("watchlist").select("code").execute()
        add([r["code"] for r in res1.data], "watchlist")
    except Exception as e:
        print(f"[watchlist] 조회 실패: {e}")

    try:
        cutoff = (now_kst() - timedelta(days=14)).isoformat()
        res2 = (
            supabase.table("tick_tracked_codes")
            .select("stock_code")
            .gte("last_requested_at", cutoff)
            .order("last_requested_at", desc=True)
            .execute()
        )
        add([r["stock_code"] for r in res2.data], "recent_tracked_14d")
    except Exception as e:
        print(f"[tick_tracked_codes] 조회 실패: {e}")

    # 위와 같은 이유로 remaining이 아니라 캐시 전체를 넘겨서 자리 낭비 방지
    if len(codes) < NXT_WS_SUBSCRIBE_LIMIT:
        add(_get_nxt_ranking_codes(NXT_RANKING_POOL_SIZE), "nxt_scan_ranking")

    return codes


@app.get("/api/ws-subscribe-preview")
def ws_subscribe_preview_endpoint():
    """
    지금 이 순간 _get_watchlist_codes()가 만들어낼 실제 웹소켓 구독 목록(최대 20개)을
    종목이 어느 소스(nxt_watchlist/watchlist/recent_tracked_14d/nxt_scan_ranking)에서
    왔는지까지 보여주는 디버그 엔드포인트. "왜 이 종목만 됐지?" 확인할 때 이걸 쓰면 됨.
    """
    try:
        items = _get_watchlist_codes_with_source()
        return {"ok": True, "limit": NXT_WS_SUBSCRIBE_LIMIT, "count": len(items), "items": items}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/track-code")
def track_code_endpoint(code: str):
    """
    Control/종목분석 페이지에서 종목을 조회할 때마다 호출 — 이 종목을 웹소켓 구독
    목록에 추가해달라고 등록함. (등록 시점부터 정밀 데이터가 쌓이기 시작 — 소급 불가)
    """
    try:
        supabase.table("tick_tracked_codes").upsert(
            {"stock_code": code, "last_requested_at": now_kst().isoformat()}
        ).execute()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-track-code")
def overseas_track_code_endpoint(symbol: str, market: str = "NAS", name: str = ""):
    """
    해외주식 패널에서 종목을 검색/조회할 때마다 호출 — 관심종목으로 명시적으로 등록 안 해도
    "최근 조회 종목"으로 자동 등록해서, 남는 구독 슬롯이 있으면 다음 웹소켓 갱신 주기(최대 5분)
    부터 정밀 추적을 시작함. 국내 /api/track-code와 완전히 같은 방식.
    ⚠️ 등록 시점부터 쌓이기 시작하는 거라 소급 적용은 안 됨 — 검색한다고 과거 데이터가
    갑자기 정밀로 바뀌진 않음.
    """
    try:
        supabase.table("overseas_tracked_codes").upsert(
            {"symbol": symbol, "market": _normalize_overseas_market(market), "name": name or symbol, "last_requested_at": now_kst().isoformat()}
        ).execute()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _ws_worker():
    """장중(07:50~20:10 KST)에만 웹소켓을 연결 유지하며 체결 데이터를 누적하는 백그라운드 작업"""
    if websockets is None:
        print("[ws_worker] websockets 패키지가 설치되어 있지 않아 실시간 누적을 건너뜁니다.")
        return
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        print("[ws_worker] KIS 키 미설정으로 실시간 누적을 건너뜁니다.")
        return

    consecutive_failures = 0
    while True:
        now = now_kst()
        start_t = now.replace(hour=7, minute=50, second=0, microsecond=0)
        end_t = now.replace(hour=20, minute=10, second=0, microsecond=0)
        if not (start_t <= now <= end_t) or now.weekday() >= 5:  # 주말 제외
            await asyncio.sleep(60)
            continue

        try:
            approval_key = get_ws_approval_key()
            # ⚠️ NXT 608개 전체 구독은 한투 웹소켓 실제 한도(등록 41건 = 종목 20개, KRX+NXT
            # 2건씩)를 훌쩍 넘어서 MAX SUBSCRIBE OVER로 계속 끊기는 문제가 있어 되돌림.
            # NXT 관심종목(무조건) + 일반 관심종목 + 최근 조회종목 + NXT 자동 스캔 상위로
            # 20개를 채움 — 자세한 우선순위는 _get_watchlist_codes() 참고.
            # ⚠️ 아래 두 함수는 Supabase/pykrx 네트워크 호출을 동기(블로킹) 방식으로 하기 때문에,
            # asyncio.to_thread로 별도 스레드에서 돌려야 이 작업이 진행되는 동안 다른 API 요청
            # (예: Control 페이지의 PC 조회 프록시)이 이벤트 루프에서 막히지 않음
            await asyncio.to_thread(_ensure_nxt_ranking_cached)
            codes = await asyncio.to_thread(_get_watchlist_codes)
            if not codes:
                print("[ws_worker] 구독할 종목이 없어 잠시 대기합니다.")
                await asyncio.sleep(300)
                continue

            async with websockets.connect(KIS_WS_URL, ping_interval=None) as ws:
                for code in codes:
                    for tr_id in ("H0STCNT0", "H0NXCNT0"):
                        sub = {
                            "header": {"approval_key": approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                            "body": {"input": {"tr_id": tr_id, "tr_key": code}},
                        }
                        await ws.send(json.dumps(sub))
                        await asyncio.sleep(0.1)
                print(f"[ws_worker] {len(codes)}개 종목({NXT_WS_SUBSCRIBE_LIMIT}개 상한) 구독 요청 완료 (KRX+NXT)")
                consecutive_failures = 0  # 연결/구독까지 성공했으니 실패 카운트 초기화

                last_flush = time.time()
                last_refresh = time.time()
                sub_error_count = 0
                while True:
                    now2 = now_kst()
                    if now2 > end_t or now2.weekday() >= 5:
                        break
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        msg = None
                    if msg:
                        if msg.startswith("0") or msg.startswith("1"):
                            _parse_realtime_message(msg)
                        elif msg.startswith("{"):
                            try:
                                parsed = json.loads(msg)
                            except Exception:
                                parsed = {}
                            tr_id = parsed.get("header", {}).get("tr_id")
                            if tr_id == "PINGPONG":
                                # ⚠️ 한투 API는 서버가 주기적으로 PINGPONG을 보내고, 클라이언트가 그대로
                                # 되돌려주지 않으면 서버가 연결을 강제로 끊음(no close frame received or
                                # sent 에러의 원인이었음). 그대로 echo 해줘야 연결이 안정적으로 유지됨.
                                await ws.send(msg)
                            elif "SUCCESS" not in msg:
                                sub_error_count += 1
                                if sub_error_count <= 20:  # 로그 폭주 방지
                                    print(f"[ws_worker] 구독 응답 이상: {msg[:300]}")

                    if time.time() - last_flush > 5:
                        await asyncio.to_thread(_flush_tick_buckets)
                        last_flush = time.time()

                    # 5분마다 관심종목 목록 갱신 — 이때 NXT 스캔 캐시도 같이 확인해서,
                    # 장중에 15:35를 넘기는 순간 자동으로 스캔 상위 종목이 채워지게 함
                    if time.time() - last_refresh > 300:
                        await asyncio.to_thread(_ensure_nxt_ranking_cached)
                        new_codes = await asyncio.to_thread(_get_watchlist_codes)
                        added = [c for c in new_codes if c not in codes]
                        for code in added:
                            for tr_id in ("H0STCNT0", "H0NXCNT0"):
                                sub = {
                                    "header": {"approval_key": approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                                    "body": {"input": {"tr_id": tr_id, "tr_key": code}},
                                }
                                await ws.send(json.dumps(sub))
                        codes = new_codes
                        last_refresh = time.time()

                _flush_tick_buckets()
        except Exception as e:
            consecutive_failures += 1
            print(f"[ws_worker] 연결 오류({consecutive_failures}번째), 10초 후 재시도: {e}")
            if consecutive_failures >= 3:
                # 정확한 만료 시간을 몰라서 6시간으로 캐싱해뒀는데, 실제로는 더 일찍 상했을 수
                # 있음 — 연속으로 계속 실패하면 캐시된 키가 문제라고 보고 강제로 새로 발급받음
                # (2026-08-07 국내장 애프터마켓 중 계속 재연결 실패하던 장애의 원인으로 추정됨)
                print("[ws_worker] 연속 실패 3회 이상 — 승인키를 강제로 재발급받습니다.")
                invalidate_ws_approval_key("domestic")
            await asyncio.sleep(10)


def _in_overseas_window(now: datetime) -> bool:
    """
    해외 거래소 체결이 있을 만한 시간대인지 확인 — "언제 여는지" 대신 "언제 확실히 다
    닫혀있는지"(16:00~21:00 KST)만 걸러내는 방식. 아시아 거래소(도쿄/홍콩/상해/심천/
    호치민/하노이, 대략 08:00~16:00 KST)와 미국 거래소(대략 21:00~07:00 KST)를 합치면
    이 구간 빼고는 거의 항상 어딘가는 열려있음.
    """
    h = now.hour
    return not (OVERSEAS_WS_IDLE_START_HOUR <= h < OVERSEAS_WS_IDLE_END_HOUR)


def _get_overseas_watchlist_symbols() -> list:
    """
    해외주식 정밀 추적 대상 — [(symbol, market), ...] 최대 OVERSEAS_WS_SUBSCRIBE_LIMIT개.
    국내 _get_watchlist_codes()와 같은 우선순위 방식:
      1) overseas_watchlist (명시적으로 등록한 관심종목) — 무조건 포함
      2) overseas_tracked_codes (최근 14일 이내 검색/조회한 종목) — 남는 슬롯 채움
    검색만 해도 자동으로 2번에 들어가서, 명시적으로 관심종목 추가 안 해도 몇 번 계속
    찾아보면 자연스럽게 정밀 추적이 시작됨 (국내 "최근 조회 종목"과 동일한 경험).
    """
    pairs: list = []
    seen = set()

    def add(rows, key_symbol="symbol", key_market="market"):
        for r in rows:
            k = (r[key_symbol], _normalize_overseas_market(r[key_market]))
            if k not in seen and len(pairs) < OVERSEAS_WS_SUBSCRIBE_LIMIT:
                seen.add(k)
                pairs.append(k)

    try:
        res1 = supabase.table("overseas_watchlist").select("symbol,market").order("added_at").execute()
        add(res1.data)
    except Exception as e:
        print(f"[overseas_watchlist] 조회 실패: {e}")

    try:
        cutoff = (now_kst() - timedelta(days=14)).isoformat()
        res2 = (
            supabase.table("overseas_tracked_codes")
            .select("symbol,market")
            .gte("last_requested_at", cutoff)
            .order("last_requested_at", desc=True)
            .execute()
        )
        add(res2.data)
    except Exception as e:
        print(f"[overseas_tracked_codes] 조회 실패: {e}")

    return pairs


async def _overseas_ws_worker():
    """
    해외 거래소가 열려있을 시간대(=16:00~21:00 KST 휴장 구간만 제외)에 웹소켓을 연결
    유지하며 해외주식 관심종목의 실시간(지연) 체결가를 누적하는 백그라운드 작업.
    국내 _ws_worker와 구조는 동일하고, 종목당 TR 1개(HDFSCNT0)만 구독하면 되는 점이 다름
    (국내는 KRX+NXT 2개 구독).
    """
    if websockets is None or not KIS_APP_KEY or not KIS_APP_SECRET:
        print("[overseas_ws_worker] websockets 미설치 또는 KIS 키 미설정으로 건너뜁니다.")
        return

    consecutive_failures = 0
    while True:
        now = now_kst()
        if not _in_overseas_window(now) or now.weekday() >= 5:
            await asyncio.sleep(60)
            continue

        try:
            approval_key = get_ws_approval_key("overseas")
            watch = await asyncio.to_thread(_get_overseas_watchlist_symbols)
            if not watch:
                print("[overseas_ws_worker] 구독할 해외주식 관심종목이 없어 잠시 대기합니다.")
                await asyncio.sleep(300)
                continue

            async with websockets.connect(KIS_WS_URL, ping_interval=None) as ws:
                for symbol, market in watch:
                    sub = {
                        "header": {"approval_key": approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                        "body": {"input": {"tr_id": "HDFSCNT0", "tr_key": f"D{market}{symbol}"}},
                    }
                    await ws.send(json.dumps(sub))
                    await asyncio.sleep(0.1)
                print(f"[overseas_ws_worker] {len(watch)}개 종목 구독 요청 완료 (HDFSCNT0)")
                consecutive_failures = 0

                last_flush = time.time()
                last_refresh = time.time()
                sub_error_count = 0
                while True:
                    now2 = now_kst()
                    if not _in_overseas_window(now2) or now2.weekday() >= 5:
                        break
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        msg = None

                    if msg:
                        if msg.startswith("0"):
                            _parse_overseas_realtime_message(msg)
                        elif msg.startswith("{"):
                            try:
                                parsed = json.loads(msg)
                            except Exception:
                                parsed = {}
                            tr_id = parsed.get("header", {}).get("tr_id")
                            if tr_id == "PINGPONG":
                                await ws.send(msg)
                            elif "SUCCESS" not in msg:
                                sub_error_count += 1
                                if sub_error_count <= 20:
                                    print(f"[overseas_ws_worker] 구독 응답 이상: {msg[:300]}")

                    if time.time() - last_flush > 15:
                        await asyncio.to_thread(_flush_overseas_tick_buckets)
                        last_flush = time.time()

                    # 5분마다 관심종목 갱신 — 새로 등록된 종목이 있으면 재연결 없이 바로 구독 추가
                    if time.time() - last_refresh > 300:
                        new_watch = await asyncio.to_thread(_get_overseas_watchlist_symbols)
                        added = [w for w in new_watch if w not in watch]
                        for symbol, market in added:
                            sub = {
                                "header": {"approval_key": approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                                "body": {"input": {"tr_id": "HDFSCNT0", "tr_key": f"D{market}{symbol}"}},
                            }
                            await ws.send(json.dumps(sub))
                        watch = new_watch
                        last_refresh = time.time()

                await asyncio.to_thread(_flush_overseas_tick_buckets)
        except Exception as e:
            consecutive_failures += 1
            print(f"[overseas_ws_worker] 연결 오류({consecutive_failures}번째), 10초 후 재시도: {e}")
            if consecutive_failures >= 3:
                print("[overseas_ws_worker] 연속 실패 3회 이상 — 승인키를 강제로 재발급받습니다.")
                invalidate_ws_approval_key("overseas")
            await asyncio.sleep(10)


@app.on_event("startup")
async def _start_ws_worker():
    asyncio.create_task(_ws_worker())
    asyncio.create_task(_overseas_ws_worker())


@app.get("/api/tick-avg")
def tick_avg_endpoint(code: str, market: str = "KRX", date: str = "", start: str = "", end: str = ""):
    """
    웹소켓으로 누적된 정밀 세력평단 조회. is_estimate=False (실제 매수/매도 체결 기반).
    market: "KRX" 또는 "NXT". date 비우면 오늘.
    """
    trade_date = date or now_kst().strftime("%Y-%m-%d")
    try:
        query = (
            supabase.table("tick_minute_flow")
            .select("*")
            .eq("stock_code", code)
            .eq("market", market)
            .eq("trade_date", trade_date)
            .order("minute", desc=False)
        )
        rows = query.execute().data
        if start:
            rows = [r for r in rows if r["minute"] >= start]
        if end:
            rows = [r for r in rows if r["minute"] <= end]

        if not rows:
            return {"error": "해당 구간에 누적된 실시간 체결 데이터가 없습니다. (웹소켓이 그 시간에 연결되어 있지 않았을 수 있음)"}

        buy_qty = sum(r["buy_qty"] for r in rows)
        buy_value = sum(r["buy_value"] for r in rows)
        sell_qty = sum(r["sell_qty"] for r in rows)
        sell_value = sum(r["sell_value"] for r in rows)

        avg = round(buy_value / buy_qty, 1) if buy_qty > 0 else 0
        sell_avg = round(sell_value / sell_qty, 1) if sell_qty > 0 else 0

        candles = [
            {
                "label": r["minute"],
                "close": r.get("last_price"),
                "avg_price": round(sum(x["buy_value"] for x in rows if x["minute"] <= r["minute"])
                                    / sum(x["buy_qty"] for x in rows if x["minute"] <= r["minute"]), 1)
                if sum(x["buy_qty"] for x in rows if x["minute"] <= r["minute"]) > 0 else 0,
            }
            for r in rows
        ]

        return {
            "code": code,
            "avg": avg,
            "sell_avg": sell_avg,
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "current_price": rows[-1].get("last_price"),
            "candles": candles,
            "is_estimate": False,  # 실제 체결 구분 기반 정밀값
        }
    except Exception as e:
        return {"error": str(e)}


# ── 프리마켓(NXT) → 정규장(KRX) → 애프터마켓(NXT) 세 구간의 경계 시각.
# NXT는 08:00~20:00 사이 계속 열려있고 KRX 정규장(09:00~15:30)과 겹치지만, 이 프로젝트는
# 정규장 시간대엔 KRX를 대표 데이터로 쓰기로 했으므로 그 구간의 NXT 체결은 통합 곡선에서
# 제외함. 15:30 정각은 KRX 정규장 마감 틱으로 간주하고, NXT 애프터마켓은 15:31부터로
# 나눠서 같은 분(minute)이 두 시장에 중복으로 잡히는 걸 막음.
_NXT_PRE_START, _NXT_PRE_END = "08:00", "08:50"
_KRX_REGULAR_START, _KRX_REGULAR_END = "09:00", "15:30"
_NXT_AFTER_START, _NXT_AFTER_END = "15:31", "20:00"


def _in_combined_session(row: dict) -> bool:
    m, mkt = row.get("minute", ""), row.get("market", "")
    if mkt == "NXT" and _NXT_PRE_START <= m <= _NXT_PRE_END:
        return True
    if mkt == "KRX" and _KRX_REGULAR_START <= m <= _KRX_REGULAR_END:
        return True
    if mkt == "NXT" and _NXT_AFTER_START <= m <= _NXT_AFTER_END:
        return True
    return False


@app.get("/api/tick-avg-combined")
def tick_avg_combined_endpoint(code: str, date: str = ""):
    """
    프리마켓(NXT 08:00~08:50) → 정규장(KRX 09:00~15:30) → 애프터마켓(NXT 15:31~20:00)을
    하나로 이어붙인 "하루 전체 통합 세력평단". tick_minute_flow에 market 컬럼으로 이미
    KRX/NXT가 같이 저장돼있어서, 시간대로만 구간을 나눠 순서대로 누적함 — 기존 KRX
    정밀 세력평단(/api/tick-avg)이나 NXT 세력평단과 별개로, 이 셋을 하나로 합친 값.
    date 비우면 오늘. 그날 하루치가 다 없으면(예: 아직 정규장 진행 중) 있는 구간까지만 계산.
    """
    trade_date = date or now_kst().strftime("%Y-%m-%d")
    try:
        res = (
            supabase.table("tick_minute_flow")
            .select("*")
            .eq("stock_code", code)
            .eq("trade_date", trade_date)
            .order("minute", desc=False)
            .execute()
        )
        rows = [r for r in res.data if _in_combined_session(r)]
        if not rows:
            return {
                "error": "해당 날짜의 프리마켓·정규장·애프터마켓 구간에 누적된 정밀 데이터가 없습니다."
            }

        rows.sort(key=lambda r: (r["minute"], r["market"]))

        buy_qty_total = sum(r["buy_qty"] for r in rows)
        buy_value_total = sum(r["buy_value"] for r in rows)
        sell_qty_total = sum(r["sell_qty"] for r in rows)
        sell_value_total = sum(r["sell_value"] for r in rows)

        avg = round(buy_value_total / buy_qty_total, 1) if buy_qty_total > 0 else 0
        sell_avg = round(sell_value_total / sell_qty_total, 1) if sell_qty_total > 0 else 0

        candles = []
        cum_buy_qty = 0.0
        cum_buy_value = 0.0
        cum_sell_qty = 0.0
        cum_sell_value = 0.0
        for r in rows:
            cum_buy_qty += r["buy_qty"]
            cum_buy_value += r["buy_value"]
            cum_sell_qty += r["sell_qty"]
            cum_sell_value += r["sell_value"]
            session = (
                "premarket" if r["market"] == "NXT" and r["minute"] <= _NXT_PRE_END else
                "aftermarket" if r["market"] == "NXT" else
                "regular"
            )
            candles.append(
                {
                    "label": r["minute"],
                    "session": session,
                    "close": r.get("last_price"),
                    "avg_price": round(cum_buy_value / cum_buy_qty, 1) if cum_buy_qty > 0 else 0,
                    "sell_avg_price": round(cum_sell_value / cum_sell_qty, 1) if cum_sell_qty > 0 else 0,
                }
            )

        return {
            "code": code,
            "trade_date": trade_date,
            "avg": avg,
            "sell_avg": sell_avg,
            "buy_qty": buy_qty_total,
            "sell_qty": sell_qty_total,
            "current_price": rows[-1].get("last_price"),
            "candles": candles,
            "is_estimate": False,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/overseas-ws-debug")
async def overseas_ws_debug(symbol: str = "AAPL", market: str = "NAS", seconds: int = 20):
    """
    디버그 전용 — 해외주식 실시간(지연) 체결가(TR: HDFSCNT0) 원본 메시지를 그대로 수집해서 반환.
    매수/매도 구분 필드가 실제로 있는지 등, 정밀 계산 가능 여부를 눈으로 확인하려고 만든 용도.
    몇 초만 구독했다가 바로 끊음 (상시 실행 아님).

    market: NAS(나스닥)/NYS(뉴욕)/AMS(아멕스)/TSE(도쿄)/HKS(홍콩)/SHS(상해)/SZS(심천)/HSX(호치민)/HNX(하노이) —
    tr_key는 "D"(지연) + market + symbol 조합
    (예: 애플 나스닥 → DNASAAPL).

    ⚠️ 미국 거래소 정규장 시간에만 체결이 발생해서 데이터가 옴 (한국시간 기준 대략 22:30~05:00,
    서머타임에 따라 1시간 당겨짐). 그 시간 밖에 호출하면 count가 0으로 나오는 게 정상.
    """
    try:
        approval_key = get_ws_approval_key("overseas")
        tr_key = f"D{market}{symbol}"
        raw_messages = []

        async with websockets.connect(KIS_WS_URL, ping_interval=None) as ws:
            sub = {
                "header": {"approval_key": approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                "body": {"input": {"tr_id": "HDFSCNT0", "tr_key": tr_key}},
            }
            await ws.send(json.dumps(sub))

            end_time = time.time() + seconds
            while time.time() < end_time:
                remaining = end_time - time.time()
                if remaining <= 0:
                    break
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break

                if msg.startswith("{"):
                    try:
                        parsed = json.loads(msg)
                        if parsed.get("header", {}).get("tr_id") == "PINGPONG":
                            await ws.send(msg)  # 여기도 PINGPONG은 그대로 되돌려줘야 연결 유지됨
                            continue
                    except Exception:
                        pass

                raw_messages.append(msg[:1000])  # 너무 길면 잘라서 저장

        return {"ok": True, "tr_key": tr_key, "count": len(raw_messages), "messages": raw_messages}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-program-debug")
def kis_program_debug(code: str):
    """
    디버그 전용: '프로그램매매 종합현황(시간)' 원본 응답을 그대로 반환.
    정확한 필드명을 실제로 확인한 뒤 정식 파싱 엔드포인트를 만들기 위한 용도.
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/comp-program-trade-today",
            tr_id="FHPPG04600101",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code,
                "FID_MRKT_CLS_CODE": "J",
                "FID_SCTN_CLS_CODE": "0",
            },
        )
        print(f"[kis_program_debug] code={code} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def fetch_program_trade_daily(code: str):
    """
    KIS '종목별 프로그램매매추이(일별)' — 최근 영업일들의 프로그램매매 순매수 추이.
    PC/키움과 무관하게 클라우드에서 바로 조회 가능 (opt10060 삽질은 폐기).
    """
    from datetime import datetime as _dt

    today_str = now_kst().strftime("%Y%m%d")
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/program-trade-by-stock-daily",
        tr_id="FHPPG04650201",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": today_str},
    )
    if data.get("rt_cd") != "0":
        raise ValueError(f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}")

    output = data.get("output", [])
    if not output:
        raise ValueError(f"{code}에 대한 프로그램매매 데이터가 없습니다.")

    rows = []
    for r in output:
        date = r.get("stck_bsop_date", "")
        if len(date) != 8:
            continue
        rows.append(
            {
                "stock_code": code,
                "trade_date": f"{date[:4]}-{date[4:6]}-{date[6:8]}",
                # 전부 원 단위로 옴 → 억원으로 변환
                "buy_amt": round(float(r.get("whol_smtn_shnu_tr_pbmn", 0)) / 1e8, 2),
                "sell_amt": round(float(r.get("whol_smtn_seln_tr_pbmn", 0)) / 1e8, 2),
                "net_amt": round(float(r.get("whol_smtn_ntby_tr_pbmn", 0)) / 1e8, 2),
                "net_chg": round(float(r.get("whol_ntby_tr_pbmn_icdc2", 0)) / 1e8, 2),
            }
        )
    return rows


@app.get("/api/sync-program-trade-daily")
def sync_program_trade_daily_endpoint(code: str):
    """
    종목분석 페이지에서 프로그램매매를 조회할 때 호출 — PC/ngrok 전혀 필요 없이
    한국투자증권 API로 바로 가져와서 program_trade_flow 테이블에 저장.
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        rows = fetch_program_trade_daily(code)
        supabase.table("program_trade_flow").upsert(rows).execute()
        return {"ok": True, "synced": len(rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-program-daily-debug")
def kis_program_daily_debug(code: str):
    """디버그 전용: '종목별 프로그램매매추이(일별)' 원본 응답 그대로 반환."""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        from datetime import datetime as _dt
        today_str = now_kst().strftime("%Y%m%d")
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/program-trade-by-stock-daily",
            tr_id="FHPPG04650201",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": today_str},
        )
        print(f"[kis_program_daily_debug] code={code} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def fetch_program_trade_tick(code: str):
    """
    KIS '종목별 프로그램매매추이(체결)' — 가장 최근 체결 시점의 누적 프로그램매매.
    저장하지 않고 매번 그 자리에서 최신값만 반환 (진짜 "지금 이 순간" 값이라 캐싱 안 함).
    """
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/program-trade-by-stock",
        tr_id="FHPPG04650101",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
    )
    if data.get("rt_cd") != "0":
        raise ValueError(f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}")

    output = data.get("output", [])
    if not output:
        raise ValueError(f"{code}에 대한 실시간 프로그램매매 데이터가 없습니다.")

    r = output[0]  # 가장 최근 체결 시점 (첫 번째 행)
    hour = r.get("bsop_hour", "")
    tick_time = f"{hour[:2]}:{hour[2:4]}:{hour[4:6]}" if len(hour) == 6 else ""

    return {
        "tick_time": tick_time,
        "buy_amt": round(float(r.get("whol_smtn_shnu_tr_pbmn", 0)) / 1e8, 2),
        "sell_amt": round(float(r.get("whol_smtn_seln_tr_pbmn", 0)) / 1e8, 2),
        "net_amt": round(float(r.get("whol_smtn_ntby_tr_pbmn", 0)) / 1e8, 2),
    }


@app.get("/api/program-trade-tick")
def program_trade_tick_endpoint(code: str):
    """
    종목분석 페이지 우측 패널(실시간) 전용 — 저장 없이 매번 최신 체결 기준 값을 그대로 반환.
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        result = fetch_program_trade_tick(code)
        return {"ok": True, **result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-program-tick-debug")
def kis_program_tick_debug(code: str):
    """디버그 전용: '종목별 프로그램매매추이(체결)' 원본 응답 그대로 반환."""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/program-trade-by-stock",
            tr_id="FHPPG04650101",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
        )
        print(f"[kis_program_tick_debug] code={code} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def estimate_force_avg(rows):
    """
    data_engine.py의 load_candles() 추정 로직(매수/매도 체결량 구분이 없을 때)을 그대로 이식.
    rows: [{"date","open","close","volume"}, ...] (오래된 순으로 정렬되어 있어야 함)
    반환: [{"label","close","avg_price"}, ...] — avg_price는 누적 "세력매수평단" 근사치
    """
    buy_value = 0.0
    buy_total = 0
    candles = []
    for r in rows:
        price = r["close"]
        open_p = r["open"] or price
        vol = r["volume"]
        if price <= 0 or vol <= 0:
            continue

        if open_p > 0 and price >= open_p:
            ratio = min((price - open_p) / open_p * 5, 0.45)
            net_buy = int(vol * ratio)
        elif open_p > 0:
            ratio = min((open_p - price) / open_p * 5, 0.45)
            net_buy = -int(vol * ratio)
        else:
            net_buy = 0

        buy_qty = max(0, (vol + net_buy) // 2)
        if buy_qty > 0:
            buy_value += price * buy_qty
            buy_total += buy_qty

        avg_price = round(buy_value / buy_total, 1) if buy_total > 0 else 0
        candles.append({"label": r["date"], "close": price, "avg_price": avg_price})
    return candles


def fetch_daily_chart(code: str, start: str, end: str):
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        tr_id="FHKST03010100",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start,
            "FID_INPUT_DATE_2": end,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        },
    )
    if data.get("rt_cd") != "0":
        raise ValueError(f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}")

    output2 = data.get("output2", [])
    if not output2:
        raise ValueError(f"{code}에 대한 일봉 데이터가 없습니다.")

    rows = [
        {
            "date": r["stck_bsop_date"],
            "open": float(r.get("stck_oprc", 0)),
            "close": float(r.get("stck_clpr", 0)),
            "volume": float(r.get("acml_vol", 0)),
        }
        for r in output2
    ]
    rows.sort(key=lambda r: r["date"])  # KIS는 최신순으로 주므로 오래된 순으로 재정렬
    return rows, data.get("output1", {})


def fetch_minute_chart(code: str, market: str = "J"):
    from datetime import datetime as _dt

    now_str = now_kst().strftime("%H%M%S")
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
        tr_id="FHKST03010200",
        params={
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": market,  # J:KRX 정규장, NX:NXT, UN:통합
            "FID_INPUT_ISCD": code,
            "FID_INPUT_HOUR_1": now_str,
            "FID_PW_DATA_INCU_YN": "Y",
        },
    )
    if data.get("rt_cd") != "0":
        raise ValueError(f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}")

    output2 = data.get("output2", [])
    if not output2:
        raise ValueError(f"{code}에 대한 분봉 데이터가 없습니다. (market={market})")

    # ⚠️ 시:분만 보고 날짜를 구분 안 하면, 이전 거래일의 같은 시간대 데이터가 섞여 들어올 수 있음
    # (예: 아침에 조회했는데 어제 애프터마켓 데이터가 오늘 것처럼 잡히는 문제) → 가장 최근 날짜만 사용
    latest_date = max(r.get("stck_bsop_date", "") for r in output2)
    output2 = [r for r in output2 if r.get("stck_bsop_date") == latest_date]

    rows = [
        {
            "date": f"{r['stck_cntg_hour'][:2]}:{r['stck_cntg_hour'][2:4]}",
            "open": float(r.get("stck_oprc", 0)),
            "close": float(r.get("stck_prpr", 0)),
            "volume": float(r.get("cntg_vol", 0)),
        }
        for r in output2
    ]
    rows.reverse()  # KIS는 최신순으로 주므로 오래된 순으로 재정렬
    return rows, data.get("output1", {})


@app.get("/api/query-avg-fallback")
def query_avg_fallback_endpoint(
    code: str,
    start: str = "",
    end: str = "",
    mode: str = "daily",
    market: str = "J",
    time_start: str = "",
    time_end: str = "",
):
    """
    Control/종목분석 페이지에서 PC(ngrok)가 응답 없을 때 자동으로 넘어오는 폴백 엔드포인트.
    실제 매수/매도 체결 분류가 아니라 "양봉/음봉 기반 추정"이라 세력평단은 근사치예요.
    mode="minute"이면 당일 분봉 기준 (start/end 무시, 항상 오늘/현재시각 기준).
    market="J"(KRX 정규장, 기본값) / "NX"(NXT) — mode="minute"일 때만 의미 있음.
    time_start/time_end: "HH:MM" 형식, 지정하면 그 구간 캔들만으로 누적평단 계산
    (장기/단기 구간, NXT 프리마켓/애프터마켓 구분 등에 사용).
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        if mode == "minute":
            rows, meta = fetch_minute_chart(code, market)
            if time_start:
                rows = [r for r in rows if r["date"] >= time_start]
            if time_end:
                rows = [r for r in rows if r["date"] <= time_end]
        else:
            rows, meta = fetch_daily_chart(code, start, end)
        candles = estimate_force_avg(rows)
        return {
            "code": code,
            "name": meta.get("hts_kor_isnm", code),
            "current_price": int(float(meta.get("stck_prpr", 0))),
            "avg": candles[-1]["avg_price"] if candles else 0,
            "candles": candles,
            "is_estimate": True,  # 프론트에서 "근사치" 라벨 표시용
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/kis-daily-chart-debug")
def kis_daily_chart_debug(code: str, start: str, end: str):
    """디버그 전용: '국내주식기간별시세(일봉)' 원본 응답 그대로 반환."""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            tr_id="FHKST03010100",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code,
                "FID_INPUT_DATE_1": start,
                "FID_INPUT_DATE_2": end,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
        )
        print(f"[kis_daily_chart_debug] code={code} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-minute-chart-debug")
def kis_minute_chart_debug(code: str, market: str = "J"):
    """디버그 전용: '주식당일분봉조회' 원본 응답 그대로 반환."""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        from datetime import datetime as _dt

        now_str = now_kst().strftime("%H%M%S")
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            tr_id="FHKST03010200",
            params={
                "FID_ETC_CLS_CODE": "",
                "FID_COND_MRKT_DIV_CODE": market,
                "FID_INPUT_ISCD": code,
                "FID_INPUT_HOUR_1": now_str,
                "FID_PW_DATA_INCU_YN": "Y",
            },
        )
        print(f"[kis_minute_chart_debug] code={code} market={market} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def fetch_shortsale(code: str, days: int = 20):
    from datetime import datetime as _dt, timedelta as _td

    today_str = now_kst().strftime("%Y%m%d")
    from_str = (now_kst() - _td(days=days * 2)).strftime("%Y%m%d")

    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/daily-short-sale",
        tr_id="FHPST04830000",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": from_str,
            "FID_INPUT_DATE_2": today_str,
        },
    )
    if data.get("rt_cd") != "0":
        raise ValueError(f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}")

    output2 = data.get("output2", [])
    if not output2:
        raise ValueError(f"{code}에 대한 공매도 데이터가 없습니다.")

    rows = []
    for r in output2[:days]:
        date = r.get("stck_bsop_date", "")
        if len(date) != 8:
            continue
        rows.append(
            {
                "stock_code": code,
                "trade_date": f"{date[:4]}-{date[4:6]}-{date[6:8]}",
                "short_qty": float(r.get("ssts_cntg_qty", 0)),
                "short_vol_pct": float(r.get("ssts_vol_rlim", 0)),
                "short_amt": round(float(r.get("ssts_tr_pbmn", 0)) / 1e8, 2),
                "short_amt_pct": float(r.get("ssts_tr_pbmn_rlim", 0)),
            }
        )
    return rows


def fetch_shorting_balance(code: str, days: int = 20):
    """
    공매도 '잔고비중' — 아까 만든 '거래비중'(KIS)이랑 완전히 다른 데이터.
    pykrx로 가져옴 (investor_flow_fetch랑 같은 소스). 실행 후 컬럼명 확인 필요할 수 있어
    디버그 프린트를 남겨둠.
    """
    today = now_kst()
    fromdate = (today - timedelta(days=days * 2)).strftime("%Y%m%d")
    todate = today.strftime("%Y%m%d")

    df = stock.get_shorting_balance_by_date(fromdate, todate, code)
    print(f"[shorting_balance] code={code} rows={len(df)} columns={list(df.columns)}")

    if df.empty:
        raise ValueError(f"{code}에 대한 공매도 잔고 데이터가 없습니다.")

    df = df.tail(days)

    rows = []
    for date_idx, row in df.iterrows():
        rows.append(
            {
                "stock_code": code,
                "trade_date": date_idx.strftime("%Y-%m-%d"),
                "balance_qty": float(row.get("공매도잔고", 0)),
                "balance_amt": round(float(row.get("공매도금액", 0)) / 1e8, 2),
                "balance_pct": float(row.get("비중", 0)),
            }
        )
    return rows


@app.get("/api/sync-shortsale-balance")
def sync_shortsale_balance_endpoint(code: str):
    """종목별 공매도 잔고비중 동기화 (pykrx, PC 필요 없음)"""
    try:
        rows = fetch_shorting_balance(code)
        supabase.table("stock_shortsale_balance").upsert(rows).execute()
        return {"ok": True, "synced": len(rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-shortsale")
def sync_shortsale_endpoint(code: str):
    """
    통합수급현황/종목분석 페이지에서 종목 검색 시 호출 — 최근 20영업일 공매도 추이를
    한국투자증권 API로 가져와서 stock_shortsale_flow 테이블에 저장. PC 필요 없음.
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        rows = fetch_shortsale(code)
        supabase.table("stock_shortsale_flow").upsert(rows).execute()
        return {"ok": True, "synced": len(rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/pykrx-shorting-balance-debug")
def pykrx_shorting_balance_debug(code: str, days: int = 40):
    """
    디버그 전용 — pykrx의 공매도 잔고비중 원본을 그대로(날짜별) 반환.
    7/24 이후로 데이터가 정말 없는 건지, 있는데 어딘가에서 걸러지는 건지 확인용.
    """
    try:
        today = now_kst()
        fromdate = (today - timedelta(days=days)).strftime("%Y%m%d")
        todate = today.strftime("%Y%m%d")
        df = stock.get_shorting_balance_by_date(fromdate, todate, code)
        rows = [
            {"date": idx.strftime("%Y-%m-%d"), **{k: (float(v) if hasattr(v, "item") else v) for k, v in row.items()}}
            for idx, row in df.iterrows()
        ]
        return {
            "ok": True,
            "code": code,
            "요청범위": f"{fromdate} ~ {todate}",
            "받은행수": len(rows),
            "컬럼": list(df.columns),
            "rows": rows,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-shortsale-debug")
def kis_shortsale_debug(code: str, start: str = "", end: str = ""):
    """디버그 전용: '국내주식 공매도 일별추이' 원본 응답 그대로 반환."""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/daily-short-sale",
            tr_id="FHPST04830000",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code,
                "FID_INPUT_DATE_1": start,
                "FID_INPUT_DATE_2": end,
            },
        )
        print(f"[kis_shortsale_debug] code={code} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/fred-rate")
def fred_rate_endpoint(series_id: str = "DGS2"):
    """
    FRED(미국 연방준비제도 공식 데이터) — 국채금리 등. 하루 단위 갱신 데이터라
    자주 폴링해도 부담 없음. series_id 예: DGS2(2년물), DGS10(10년물)
    """
    if not FRED_API_KEY:
        return {"error": "FRED_API_KEY 환경변수가 설정되어 있지 않습니다."}
    try:
        res = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": series_id,
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 10,
            },
            timeout=10,
        )
        res.raise_for_status()
        data = res.json()
        obs = [o for o in data.get("observations", []) if o.get("value") not in (".", None)]
        if not obs:
            return {"error": f"{series_id}에 대한 관측치가 없습니다."}

        latest = float(obs[0]["value"])
        prev = float(obs[1]["value"]) if len(obs) > 1 else latest
        change = round(latest - prev, 4)
        change_pct = round((change / prev) * 100, 2) if prev else 0

        return {"price": latest, "change": change, "change_pct": change_pct, "date": obs[0]["date"]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/kis-rate")
def kis_rate_endpoint(bcdt_code: str, div_cls: str = "0"):
    """
    금리 종합(국내채권/금리) — 저장 없이 그때그때 최신값 반환.
    div_cls="0": 국내채권/금리 (예: Y0106=국고채 10년)
    div_cls="1": 해외금리지표 (예: Y0202=미국 10년T-NOTE)
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/comp-interest",
            tr_id="FHPST07020000",
            params={
                "FID_COND_MRKT_DIV_CODE": "I",
                "FID_COND_SCR_DIV_CODE": "20702",
                "FID_DIV_CLS_CODE": div_cls,
                "FID_DIV_CLS_CODE1": "",
            },
        )
        if data.get("rt_cd") != "0":
            return {"error": f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}"}

        output1 = data.get("output1", [])
        row = next((r for r in output1 if r.get("bcdt_code") == bcdt_code), None)
        if not row:
            return {"error": f"{bcdt_code} 코드를 찾을 수 없습니다."}

        price = float(row.get("bond_mnrt_prpr", 0))
        change = float(row.get("bond_mnrt_prdy_vrss", 0))
        sign = row.get("prdy_vrss_sign", "")
        if sign in ("5", "4"):  # 하락 부호
            change = -abs(change)
        pct = float(row.get("prdy_ctrt", 0))
        return {"name": row.get("hts_kor_isnm", bcdt_code), "price": price, "change": change, "change_pct": pct}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/kis-interest-debug")
def kis_interest_debug(div_cls: str = "", div_cls1: str = ""):
    """디버그 전용: '금리 종합(국내채권/금리)' 원본 응답 그대로 반환."""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/comp-interest",
            tr_id="FHPST07020000",
            params={
                "FID_COND_MRKT_DIV_CODE": "I",
                "FID_COND_SCR_DIV_CODE": "20702",
                "FID_DIV_CLS_CODE": div_cls,
                "FID_DIV_CLS_CODE1": div_cls1,
            },
        )
        print(f"[kis_interest_debug] div_cls={div_cls} div_cls1={div_cls1} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/fmp-commodities-debug")
def fmp_commodities_debug():
    """디버그 전용: FMP 지원 원자재 심볼 목록 (니켈 심볼 확인용)."""
    if not FMP_API_KEY:
        return {"ok": False, "error": "FMP_API_KEY 환경변수가 설정되어 있지 않습니다."}
    try:
        res = requests.get(
            "https://financialmodelingprep.com/stable/commodities-list",
            params={"apikey": FMP_API_KEY},
            timeout=10,
        )
        print(f"[fmp_commodities_debug] status={res.status_code} body={res.text[:2000]}")
        return {"ok": True, "status": res.status_code, "raw": res.text[:3000]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/fmp-nickel-debug")
def fmp_nickel_debug(symbol: str = "NIUSD"):
    """디버그 전용: FMP 니켈 시세 조회 시도 (심볼 확실치 않아 파라미터로 바꿔볼 수 있게 함)."""
    if not FMP_API_KEY:
        return {"ok": False, "error": "FMP_API_KEY 환경변수가 설정되어 있지 않습니다."}
    try:
        res = requests.get(
            "https://financialmodelingprep.com/stable/quote",
            params={"symbol": symbol, "apikey": FMP_API_KEY},
            timeout=10,
        )
        print(f"[fmp_nickel_debug] symbol={symbol} status={res.status_code} body={res.text[:1000]}")
        return {"ok": True, "status": res.status_code, "raw": res.text[:2000]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/krx-fut-price")
def krx_fut_price_endpoint(prod_name: str = "코스피200 선물"):
    """
    KRX Open API '선물 일별매매정보'에서 지정 상품(기본: 코스피200 선물)의
    정규장 데이터 중 거래량이 가장 많은(=근월물) 계약을 골라 반환.
    저장 없이 매번 그 자리에서 조회 (하루 10,000회 한도라 30초 폴링에도 넉넉함).
    """
    if not KRX_AUTH_KEY:
        return {"error": "KRX_AUTH_KEY 환경변수가 설정되어 있지 않습니다."}
    try:
        rows = []
        for back in range(5):  # 주말/공휴일 대비 최대 5일 소급
            bas_dd = (now_kst() - timedelta(days=back)).strftime("%Y%m%d")
            res = requests.get(
                "https://data-dbg.krx.co.kr/svc/apis/drv/fut_bydd_trd",
                headers={"AUTH_KEY": KRX_AUTH_KEY},
                params={"basDd": bas_dd},
                timeout=10,
            )
            res.raise_for_status()
            rows = res.json().get("OutBlock_1", [])
            if rows:
                break

        candidates = [
            r for r in rows
            if r.get("PROD_NM") == prod_name and r.get("MKT_NM") == "정규" and r.get("TDD_CLSPRC")
        ]
        if not candidates:
            return {"error": f"{prod_name} 데이터를 찾을 수 없습니다."}

        best = max(candidates, key=lambda r: float(r.get("ACC_TRDVOL", 0) or 0))
        price = float(best["TDD_CLSPRC"])
        change = float(best["CMPPREVDD_PRC"] or 0)
        prev_price = price - change
        change_pct = round((change / prev_price) * 100, 2) if prev_price else 0

        return {"price": price, "change": change, "change_pct": change_pct, "name": best.get("ISU_NM", prod_name)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/krx-fut-debug")
def krx_fut_debug(bas_dd: str = ""):
    """디버그 전용: KRX Open API '선물 일별매매정보' 원본 응답 그대로 반환."""
    if not KRX_AUTH_KEY:
        return {"ok": False, "error": "KRX_AUTH_KEY 환경변수가 설정되어 있지 않습니다."}
    try:
        bas_dd = bas_dd or now_kst().strftime("%Y%m%d")
        res = requests.get(
            "http://data-dbg.krx.co.kr/svc/apis/drv/fut_bydd_trd",
            headers={"AUTH_KEY": KRX_AUTH_KEY},
            params={"basDd": bas_dd},
            timeout=10,
        )
        print(f"[krx_fut_debug] status={res.status_code} bas_dd={bas_dd} body={res.text[:2000]}")
        return {"ok": True, "status": res.status_code, "raw": res.text[:3000]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-fut-price")
def kis_fut_price_endpoint(iscd: str = "101000"):
    """
    한투 API '선물옵션 분봉조회'의 output1(현재가 요약)만 써서 코스피200 선물
    실시간 시세를 반환. iscd="101000"은 항상 최근월물(거래량 최다)을 자동으로 가리킴.
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        now_str = now_kst().strftime("%H%M%S")
        today_str = now_kst().strftime("%Y%m%d")
        data = kis_get(
            "/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice",
            tr_id="FHKIF03020200",
            params={
                "FID_COND_MRKT_DIV_CODE": "F",
                "FID_INPUT_ISCD": iscd,
                "FID_HOUR_CLS_CODE": "60",
                "FID_PW_DATA_INCU_YN": "N",
                "FID_FAKE_TICK_INCU_YN": "N",
                "FID_INPUT_DATE_1": today_str,
                "FID_INPUT_HOUR_1": now_str,
            },
        )
        if data.get("rt_cd") != "0":
            return {"error": f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}"}

        o1 = data.get("output1", {})
        if not o1:
            return {"error": "output1이 비어 있습니다."}

        price = float(o1.get("futs_prpr", 0))
        change = float(o1.get("futs_prdy_vrss", 0))
        sign = o1.get("prdy_vrss_sign", "")
        if sign in ("4", "5"):  # 하락 부호
            change = -abs(change)
        change_pct = float(o1.get("futs_prdy_ctrt", 0))
        return {
            "price": price,
            "change": change,
            "change_pct": change_pct,
            "name": o1.get("hts_kor_isnm", "코스피200 선물"),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/kis-fut-minute-debug")
def kis_fut_minute_debug(iscd: str = "101000", hour_cls: str = "60"):
    """
    디버그 전용: '선물옵션 분봉조회' 원본 응답 그대로 반환.
    iscd 기본값 101000 = "최근월물 연속코드" 주장을 테스트해봄 (틀릴 수도 있음).
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        now_str = now_kst().strftime("%H%M%S")
        today_str = now_kst().strftime("%Y%m%d")
        data = kis_get(
            "/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice",
            tr_id="FHKIF03020200",
            params={
                "FID_COND_MRKT_DIV_CODE": "F",
                "FID_INPUT_ISCD": iscd,
                "FID_HOUR_CLS_CODE": hour_cls,
                "FID_PW_DATA_INCU_YN": "N",
                "FID_FAKE_TICK_INCU_YN": "N",
                "FID_INPUT_DATE_1": today_str,
                "FID_INPUT_HOUR_1": now_str,
            },
        )
        print(f"[kis_fut_minute_debug] iscd={iscd} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-updown-debug")
def kis_updown_debug(mkop_cls: str = "0", sort_cls: str = "6"):
    """
    디버그 전용: '국내주식 예상체결 상승/하락상위' 원본 응답 그대로 반환.
    mkop_cls: 0=장전예상, 1=장마감예상 (평일 08:30~09:00 / 15:20~15:30에 테스트해야 의미 있음)
    sort_cls: 6=거래대금 기준 정렬 (기본값)
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/ranking/exp-trans-updown",
            tr_id="FHPST01820000",
            params={
                "fid_rank_sort_cls_code": sort_cls,
                "fid_cond_mrkt_div_code": "J",
                "fid_cond_scr_div_code": "20182",
                "fid_input_iscd": "0000",
                "fid_div_cls_code": "0",
                "fid_aply_rang_prc_1": "",
                "fid_vol_cnt": "",
                "fid_pbmn": "",
                "fid_blng_cls_code": "0",
                "fid_mkop_cls_code": mkop_cls,
            },
        )
        print(f"[kis_updown_debug] mkop_cls={mkop_cls} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-member-debug")
def kis_member_debug(code: str):
    """
    디버그 전용: '주식현재가 회원사'(회원사별 매매동향) 원본 응답을 그대로 반환.
    정확한 필드명을 실제로 확인한 뒤 정식 파싱 엔드포인트를 만들기 위한 용도.
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/inquire-member",
            tr_id="FHKST01010600",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
        )
        print(f"[kis_member_debug] code={code} raw={data}")
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def fetch_broker_flow(code: str):
    """
    KIS '주식현재가 회원사'(FHKST01010600)에서 매도/매수 상위 5개 회원사(증권사 창구)를 파싱.
    같은 증권사가 매도/매수 양쪽에 다 나올 수 있는 구조라서, 순매수 하나로 합치지 않고
    '매도 상위' / '매수 상위'를 각각 그대로 저장함.
    """
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-member",
        tr_id="FHKST01010600",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
    )
    if data.get("rt_cd") != "0":
        raise ValueError(f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}")

    output = data.get("output")
    if not output:
        raise ValueError("KIS 응답에 output이 없습니다.")
    row = output[0] if isinstance(output, list) else output

    # KIS 회원사 데이터엔 금액이 없어서, 현재가(종가)를 곱해 추정 거래금액을 계산 (근사치)
    price = None
    try:
        ymd = _latest_trading_day()
        ohlcv = stock.get_market_ohlcv_by_date(ymd, ymd, code)
        if not ohlcv.empty:
            price = float(ohlcv.iloc[-1].get("종가", 0)) or None
    except Exception as e:
        print(f"[fetch_broker_flow] 현재가 조회 실패, est_amount 없이 진행: {e}")

    trade_date = now_kst().strftime("%Y-%m-%d")
    rows = []
    for i in range(1, 6):
        sell_qty = float(row.get(f"total_seln_qty{i}", 0) or 0)
        rows.append(
            {
                "stock_code": code,
                "trade_date": trade_date,
                "side": "sell",
                "rank": i,
                "broker_name": row.get(f"seln_mbcr_name{i}", ""),
                "qty": sell_qty,
                "pct": float(row.get(f"seln_mbcr_rlim{i}", 0) or 0),
                "is_foreign": row.get(f"seln_mbcr_glob_yn_{i}") == "Y",
                "est_amount": round(sell_qty * price / 1e8, 2) if price else None,  # 억원
            }
        )
        buy_qty = float(row.get(f"total_shnu_qty{i}", 0) or 0)
        rows.append(
            {
                "stock_code": code,
                "trade_date": trade_date,
                "side": "buy",
                "rank": i,
                "broker_name": row.get(f"shnu_mbcr_name{i}", ""),
                "qty": buy_qty,
                "pct": float(row.get(f"shnu_mbcr_rlim{i}", 0) or 0),
                "is_foreign": row.get(f"shnu_mbcr_glob_yn_{i}") == "Y",
                "est_amount": round(buy_qty * price / 1e8, 2) if price else None,  # 억원
            }
        )
    return rows


@app.get("/api/sync-broker-flow")
def sync_broker_flow_endpoint(code: str):
    """
    통합수급현황 탭에서 종목 검색 시 호출 — 오늘자 회원사별(증권사 창구) 매도/매수
    상위 5개사를 broker_daily_flow 테이블에 저장. (한국투자증권 API 사용, 실데이터)
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        rows = fetch_broker_flow(code)
        supabase.table("broker_daily_flow").upsert(rows).execute()
        return {"ok": True, "synced": len(rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def fetch_investor_flow(code: str, days: int = 20):
    today = now_kst()
    fromdate = (today - timedelta(days=days * 2)).strftime("%Y%m%d")  # 주말 감안 여유있게
    todate = today.strftime("%Y%m%d")

    df = stock.get_market_trading_value_by_date(fromdate, todate, code, detail=True)

    # 디버그용: Render 대시보드 Logs 탭에서 이 출력을 확인할 수 있음
    print(f"[investor_flow] code={code} fromdate={fromdate} todate={todate} rows={len(df)}")
    print(f"[investor_flow] columns={list(df.columns)}")

    if df.empty:
        raise ValueError(
            f"KRX에서 {code} 데이터를 가져오지 못했습니다 (조회기간 {fromdate}~{todate}, 응답 0행). "
            "종목코드가 맞는지, 최근 상장/거래정지 종목은 아닌지 확인해주세요."
        )

    df = df.tail(days)  # 최근 N 영업일만

    rows = []
    for date_idx, row in df.iterrows():
        institution = (
            row.get("금융투자", 0)
            + row.get("보험", 0)
            + row.get("투신", 0)
            + row.get("사모", 0)
            + row.get("은행", 0)
            + row.get("기타금융", 0)
        )
        # pykrx 버전에 따라 '외국인' 또는 '외국인합계'로 컬럼명이 다를 수 있어 둘 다 시도
        foreign = row.get("외국인", None)
        if foreign is None:
            foreign = row.get("외국인합계", 0)
        rows.append(
            {
                "stock_code": code,
                "trade_date": date_idx.strftime("%Y-%m-%d"),
                # pykrx 거래대금은 '원' 단위 → 억원으로 변환 (1억 = 1e8)
                "foreign_net": round(float(foreign) / 1e8, 2),
                "institution_net": round(float(institution) / 1e8, 2),
                "individual_net": round(float(row.get("개인", 0)) / 1e8, 2),
                "pension_net": round(float(row.get("연기금", 0)) / 1e8, 2),
            }
        )
    return rows


@app.get("/api/sync-investor-flow")
def sync_investor_flow_endpoint(code: str):
    """
    웹사이트(FundFlowChart)가 종목 검색 시 이 엔드포인트를 직접 호출합니다.
    기존 live_server.py의 /api/sync-investor-flow와 응답 형식이 동일해서
    프론트엔드 코드는 URL만 바꾸면 그대로 씁니다.
    """
    try:
        rows = fetch_investor_flow(code)
        supabase.table("stock_investor_flow").upsert(rows).execute()
        return {"ok": True, "synced": len(rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _latest_trading_day() -> str:
    """오늘이 주말/공휴일이라 데이터가 없을 수 있어서, 최근 영업일을 찾아 반환 (YYYYMMDD)"""
    from datetime import date as _date

    d = now_kst()
    for _ in range(10):
        ymd = d.strftime("%Y%m%d")
        # 코스피 지수 하나로 그날 데이터가 있는지 간단히 확인
        test = stock.get_index_ohlcv_by_date(ymd, ymd, "1001")
        if not test.empty:
            return ymd
        d -= timedelta(days=1)
    raise ValueError("최근 10일 내 KRX 영업일을 찾지 못했습니다")


def fetch_volume_top30(limit: int = 30):
    ymd = _latest_trading_day()

    df = stock.get_market_ohlcv_by_ticker(ymd, market="ALL")
    print(f"[volume_top30] date={ymd} rows={len(df)} columns={list(df.columns)}")

    if df.empty:
        raise ValueError(f"KRX에서 {ymd} 시세 데이터를 가져오지 못했습니다.")

    df = df.sort_values("거래대금", ascending=False).head(limit)

    rows = []
    for i, (code, row) in enumerate(df.iterrows(), start=1):
        try:
            name = stock.get_market_ticker_name(code)
        except Exception:
            name = code
        rows.append(
            {
                "rank": i,
                "code": code,
                "name": name,
                "price": int(row.get("종가", 0)),
                "change_pct": float(row.get("등락률", 0)),
                # pykrx 거래대금은 '원' 단위로 나옴 → 억원으로 변환
                "trading_value": round(float(row.get("거래대금", 0)) / 1e8, 1),
            }
        )
    return rows


def fetch_sector_volume(limit: int = 30):
    ymd = _latest_trading_day()

    all_rows = []
    for market in ("KOSPI", "KOSDAQ"):
        tickers = stock.get_index_ticker_list(ymd, market=market)
        for ticker in tickers:
            try:
                name = stock.get_index_ticker_name(ticker)
                ohlcv = stock.get_index_ohlcv_by_date(ymd, ymd, ticker)
                if ohlcv.empty:
                    continue
                trading_value = float(ohlcv.iloc[0].get("거래대금", 0))
                all_rows.append({"sector_name": name, "trading_value": trading_value})
            except Exception as e:
                print(f"[sector_volume] {market} {ticker} 스킵: {e}")
                continue

    print(f"[sector_volume] date={ymd} 업종 수={len(all_rows)}")

    if not all_rows:
        raise ValueError(f"KRX에서 {ymd} 업종별 거래대금 데이터를 가져오지 못했습니다.")

    all_rows.sort(key=lambda r: r["trading_value"], reverse=True)
    top = all_rows[:limit]

    rows = []
    for i, r in enumerate(top, start=1):
        rows.append(
            {
                "rank": i,
                "sector_name": r["sector_name"],
                # 억원으로 변환
                "trading_value": round(r["trading_value"] / 1e8, 1),
            }
        )
    return rows


@app.get("/api/sync-market-volume")
def sync_market_volume_endpoint():
    """
    거래대금 TOP30 + 섹터별 거래대금 순위를 KRX에서 새로 가져와
    기존 volume_top30 / sector_volume 테이블을 통째로 갱신합니다.
    (VolumeTop30Table.tsx가 읽는 테이블과 스키마 그대로 맞춤 — 프론트엔드 수정 불필요)
    """
    try:
        volume_rows = fetch_volume_top30()
        sector_rows = fetch_sector_volume()

        # 순위 기반 테이블이라, 통째로 지우고 새로 넣는 방식이 어제 순위가 남는 문제를 방지함
        supabase.table("volume_top30").delete().neq("rank", -1).execute()
        supabase.table("volume_top30").insert(volume_rows).execute()

        supabase.table("sector_volume").delete().neq("rank", -1).execute()
        supabase.table("sector_volume").insert(sector_rows).execute()

        return {"ok": True, "volume_synced": len(volume_rows), "sector_synced": len(sector_rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


INST_TYPE_COLUMNS = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금", "기타법인", "외국인", "기타외국인", "개인"]


def fetch_institution_type_flow(code: str, days: int = 60):
    today = now_kst()
    fromdate = (today - timedelta(days=days * 2)).strftime("%Y%m%d")  # 주말 감안 여유있게
    todate = today.strftime("%Y%m%d")

    df = stock.get_market_trading_value_by_date(fromdate, todate, code, detail=True)
    print(f"[institution_type_flow] code={code} rows={len(df)} columns={list(df.columns)}")

    if df.empty:
        raise ValueError(f"KRX에서 {code} 데이터를 가져오지 못했습니다 (조회기간 {fromdate}~{todate}).")

    df = df.tail(days)

    rows = []
    for date_idx, row in df.iterrows():
        trade_date = date_idx.strftime("%Y-%m-%d")
        for inst_type in INST_TYPE_COLUMNS:
            rows.append(
                {
                    "stock_code": code,
                    "trade_date": trade_date,
                    "inst_type": inst_type,
                    "amount": round(float(row.get(inst_type, 0)) / 1e8, 2),  # 원 → 억원
                }
            )
    return rows


@app.get("/api/sync-institution-type")
def sync_institution_type_endpoint(code: str):
    """
    통합수급현황 탭에서 종목 검색 시 호출 — 기관 유형별(은행/보험/투신/연기금 등)
    최근 60영업일 순매수를 institution_type_flow 테이블에 저장합니다.
    (외국계 창구별 데이터는 pykrx로 얻을 수 없어 이 엔드포인트에 포함되지 않음)
    """
    try:
        rows = fetch_institution_type_flow(code)
        supabase.table("institution_type_flow").upsert(rows).execute()
        return {"ok": True, "synced": len(rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/ping")
def ping():
    return {"ok": True}

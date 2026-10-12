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
import re
from datetime import datetime, timedelta, timezone

import requests

try:
    from bs4 import BeautifulSoup  # 네이버 증권 NXT 거래상위 스크래핑용 — requirements.txt에 beautifulsoup4 필요
except ImportError:
    BeautifulSoup = None
    print("[naver_scrape] beautifulsoup4가 설치되어 있지 않아 NXT 거래상위 스크래핑을 건너뜁니다.")
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ── Render 서버는 UTC로 돌아가서, now_kst()를 그냥 쓰면 한국시간보다 9시간
# 느리게 나옴 (특히 자정 근처엔 날짜까지 하루 밀림). 반드시 이 함수로만 "지금"을 구함.
KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    return datetime.now(KST)


# ⚠️ pykrx는 import되는 순간 내부적으로 KRX 웹사이트에 자동 로그인을 시도함
# (KRX_ID/KRX_PW 환경변수를 읽어서). KRX 서버가 그 순간 잠깐이라도 이상한 응답(빈 응답,
# 오류 페이지 등)을 주면 pykrx가 예외 처리 없이 그대로 죽는데, 이게 import 시점에
# 터지면 uvicorn이 앱을 아예 못 띄우고 서비스 전체가 죽어버림 (2026-08-06, 2026-08-08
# 두 번 실제 발생한 장애). 근데 정작 저희가 실제로 쓰는 pykrx 기능(일봉 조회, 종목명
# 조회 등)은 이 로그인이랑 전혀 무관함 — 채권 관련 서브모듈이 부수적으로 트리거하는
# 로그인일 뿐이라, 두 가지로 방어함:
#   1) import 하는 그 순간만 KRX_ID/KRX_PW를 잠깐 숨겨서 애초에 로그인 시도 자체를 스킵
#      (저희 자체 KRX 로그인 함수는 매번 호출 시점에 os.environ에서 다시 읽으니 영향 없음)
#   2) 그래도 실패하면(재시도까지 다 실패해도) 더 이상 죽지 않고 stock=None으로 두고
#      계속 부팅 — 이러면 pykrx 쓰는 개별 기능만 그때그때 에러 응답을 주고, 나머지
#      (국내/NXT/해외 웹소켓, 네이버 스크래핑 등)는 정상 작동함
_PYKRX_IMPORT_RETRIES = 5
_saved_krx_id = os.environ.pop("KRX_ID", None)
_saved_krx_pw = os.environ.pop("KRX_PW", None)
stock = None
for _pykrx_attempt in range(1, _PYKRX_IMPORT_RETRIES + 1):
    try:
        from pykrx import stock

        break
    except Exception as _pykrx_err:
        print(f"[pykrx import] {_pykrx_attempt}/{_PYKRX_IMPORT_RETRIES}번째 시도 실패: {_pykrx_err}")
        if _pykrx_attempt == _PYKRX_IMPORT_RETRIES:
            print("[pykrx import] 재시도 다 실패 — pykrx 없이 나머지 서비스는 계속 띄웁니다.")
        else:
            time.sleep(5)
if _saved_krx_id is not None:
    os.environ["KRX_ID"] = _saved_krx_id
if _saved_krx_pw is not None:
    os.environ["KRX_PW"] = _saved_krx_pw

from supabase import create_client


def _krx_df(fn, *args, retries: int = 3, wait: float = 3.0, **kwargs):
    """
    pykrx 호출을 감싸는 재시도 래퍼.
    ⚠️ KRX 로그인 세션이 만료된 상태에서 첫 요청을 보내면 pykrx가 에러 대신 "빈 결과"를
    돌려주고, 그 사이에 뒤에서 재로그인함(Render 로그의 "KRX 세션 만료, 재로그인 시도..."
    → "KRX 세션 갱신 완료."). 그래서 바로 다시 요청하면 정상적으로 데이터가 옴.
    이걸 안 해서 세션이 만료된 직후의 요청(하루 1번 도는 자동 동기화 포함)이
    "KRX에서 데이터를 가져오지 못했습니다"로 실패하던 문제를 막음 (2026-10-08).
    """
    last = None
    for attempt in range(retries):
        try:
            last = fn(*args, **kwargs)
            if last is not None and not getattr(last, "empty", False):
                return last
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"[krx_retry] {getattr(fn, '__name__', fn)} {attempt + 1}번째 실패, 재시도: {e}")
        if attempt < retries - 1:
            time.sleep(wait)
    return last


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

# 네이버 증권 "NXT 거래상위" 스크래핑용 — 이건 위 NXT_SCAN_HOUR(608종목 스캔, KRX 마감
# 기준 15:35)와 완전히 다른 목적/데이터라 시각도 따로 둠. NXT 애프터마켓 마감(20:00)
# 직후로 잡음.
NAVER_NXT_SCAN_HOUR = 20
NAVER_NXT_SCAN_MINUTE = 5

# "실시간 랭킹" 해외 컬럼 — 미국/일본/홍콩·중국 3개 그룹, 그룹마다 마감 시각이 완전히
# 달라서 각자 따로 스냅샷 시각을 잡음. (마켓코드 리스트, 마감 기준 시, 분)
# 미국은 서머타임 따라 마감이 밀리는데, 넉넉하게 06:30로 잡아서 어느 쪽이든 이미 마감된
# 뒤가 되게 함. 일본/홍콩·중국은 한국이랑 시차가 없거나 적어서 비교적 명확함.
OVERSEAS_RANKING_GROUPS = {
    "US": {"markets": ["NAS", "NYS", "AMS"], "hour": 6, "minute": 30},
    "JP": {"markets": ["TSE"], "hour": 15, "minute": 10},
    "HK_CN": {"markets": ["HKS", "SHS", "SZS"], "hour": 16, "minute": 30},
}

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


# 실시간 연결 상태 (데이터 상태 알림용) — 마지막으로 구독 성공한 시각 / 마지막 체결 받은 시각
_ws_status = {"last_subscribed": 0.0, "last_tick": 0.0}


def _add_tick(code: str, market: str, hhmmss: str, bsop_date: str, price: float, qty: float, ccld_dvsn: str):
    _ws_status["last_tick"] = time.time()
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


_VALID_CODE = re.compile(r"^[0-9A-Z]{6}$")  # 005930, 0126Z0 같은 6자리 종목코드
_VALID_HHMMSS = re.compile(r"^([01][0-9]|2[0-3])[0-5][0-9][0-5][0-9]$")
_stride_warned: dict = {}
_bad_tick_count = {"n": 0}


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

    # ⚠️ 한 메시지에 체결이 여러 건(count>1) 묶여 올 때, 한 건당 필드 수를 46개로 고정해서 잘랐더니
    # 실제 필드 수가 다른 경우(시장/TR별로 다를 수 있음) 두 번째 건부터 위치가 밀려서 "2", "0.25" 같은
    # 엉뚱한 종목코드와 "99:81" 같은 깨진 시각이 저장되고 있었음 (그날 데이터의 10~30%).
    # → 전체 필드 수를 건수로 나눠서 실제 한 건당 필드 수를 구하고, 종목코드/시각 형식이 맞는 것만 저장함.
    n_records = max(count, 1)
    stride = TICK_FIELD_COUNT
    if n_records > 1 and len(fields) % n_records == 0:
        stride = len(fields) // n_records
    if stride != TICK_FIELD_COUNT and _stride_warned.get(tr_id) != stride:
        _stride_warned[tr_id] = stride
        print(f"[ws_parse] {tr_id} 한 건당 필드 수가 {TICK_FIELD_COUNT}가 아니라 {stride}개로 와서 그에 맞춰 자름")

    for i in range(n_records):
        start = i * stride
        chunk = fields[start : start + stride]
        if len(chunk) < IDX_CCLD_DVSN + 1:
            continue
        try:
            code = chunk[IDX_CODE]
            hhmmss = chunk[IDX_HOUR]
            price = float(chunk[IDX_PRICE])
            cntg_vol = float(chunk[IDX_CNTG_VOL])
            ccld_dvsn = chunk[IDX_CCLD_DVSN]
            bsop_date = chunk[IDX_BSOP_DATE] if len(chunk) > IDX_BSOP_DATE else ""
        except (ValueError, IndexError):
            continue
        if not _VALID_CODE.match(code) or not _VALID_HHMMSS.match(hhmmss):
            _bad_tick_count["n"] += 1
            if _bad_tick_count["n"] in (1, 100, 1000, 10000):
                print(f"[ws_parse] 형식이 깨진 체결 {_bad_tick_count['n']}건째 버림 (예: code={code!r}, time={hhmmss!r})")
            continue
        if not re.fullmatch(r"\d{8}", bsop_date or ""):
            bsop_date = now_kst().strftime("%Y%m%d")
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
    df = _krx_df(stock.get_market_ohlcv_by_ticker, ymd, market="ALL")
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
        name = _safe_ticker_name(r["code"])
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


def _is_preferred_stock_name(name: str) -> bool:
    """
    국내 우선주 이름 패턴 — '삼성전자우', 'LG화학우B', '현대차2우B' 등은 끝이
    "우"(+등급 알파벳 하나) 로 끝남. 100% 완벽한 판별은 아니지만 실무에서 흔히 쓰는 방식.
    """
    if not isinstance(name, str):
        return False
    return bool(name) and bool(re.search(r"우[A-Z]?$", name))


def _get_krx_etf_codes() -> set:
    """KRX ETF 종목코드 집합 — "실시간 랭킹"에서 ETF 제외용. pykrx 자체가 안 되어있으면 빈 집합 반환."""
    if stock is None:
        return set()
    try:
        ymd = _latest_trading_day()
        return set(stock.get_etf_ticker_list(ymd))
    except Exception as e:
        print(f"[ranking] ETF 목록 조회 실패, ETF 제외 없이 진행: {e}")
        return set()


def _naver_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }


def _naver_stock_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://m.stock.naver.com/",
    }


# ── 종목코드 → 종목명 (안전 버전) ─────────────────────────────────────────────
# ⚠️ pykrx의 get_market_ticker_name()은 실패해도 예외를 안 던지고 "빈 DataFrame"을
# 돌려줄 때가 있음 (Render 로그의 "Error occurred in get_stock_name: 'NoneType' object
# is not subscriptable"). 특히 0126Z0 같은 영문 섞인 신규 종목코드에서 잘 터짐.
# 그 DataFrame이 이름 자리에 들어가면 _is_preferred_stock_name()에서 "The truth value of
# a DataFrame is ambiguous" 에러로 랭킹 스캔 전체가 죽고, Supabase 저장도 실패함
# (2026-08 중순부터 KRX 랭킹/NXT 스캔이 매일 실패하던 원인). 그래서 이름은 항상 이 함수로만 구함:
#   1) pykrx 결과가 "진짜 문자열"일 때만 사용
#   2) 아니면 Supabase에 이미 저장된 이름(stock_fundamentals / sector_stocks)에서 찾음
#   3) 그래도 없으면(allow_remote=True일 때만) 네이버 증권 API로 1건 조회
#   4) 다 실패하면 종목코드를 그대로 이름으로 씀 — 절대 예외를 던지지 않음
_name_cache: dict = {}
_name_cache_loaded_at = 0.0
_name_cache_lock = threading.Lock()


def _load_name_cache():
    global _name_cache_loaded_at
    with _name_cache_lock:
        if _name_cache and time.time() - _name_cache_loaded_at < 6 * 3600:
            return
        loaded = {}
        for table in ("sector_stocks", "stock_fundamentals"):
            start = 0
            while True:
                try:
                    res = supabase.table(table).select("code,name").range(start, start + 999).execute()
                except Exception as e:
                    print(f"[name_cache] {table} 조회 실패: {e}")
                    break
                for r in res.data or []:
                    code, name = r.get("code"), r.get("name")
                    if code and isinstance(name, str) and name and name != code:
                        loaded[code] = name
                if not res.data or len(res.data) < 1000:
                    break
                start += 1000
        _name_cache.update(loaded)
        _name_cache_loaded_at = time.time()
        print(f"[name_cache] 종목명 {len(_name_cache)}개 로드")


def _safe_ticker_name(code: str, allow_remote: bool = True) -> str:
    if not code:
        return code
    if stock is not None:
        try:
            name = stock.get_market_ticker_name(code)
            if isinstance(name, str) and name.strip():
                return name.strip()
        except Exception:
            pass

    _load_name_cache()
    if code in _name_cache:
        return _name_cache[code]

    if allow_remote:
        try:
            res = requests.get(
                f"https://m.stock.naver.com/api/stock/{code}/basic", headers=_naver_stock_headers(), timeout=5
            )
            if res.ok:
                name = (res.json() or {}).get("stockName")
                if isinstance(name, str) and name.strip():
                    _name_cache[code] = name.strip()
                    return name.strip()
        except Exception as e:
            print(f"[name_lookup] {code} 네이버 조회 실패: {e}")
    return code


def fetch_naver_theme_list(limit: int = 25):
    """
    "업종분석" 국내 탭용 — 네이버 증권 인기 테마 목록.
    2026-09 네이버가 finance.naver.com(HTML) → stock.naver.com(Next.js, JSON API)로
    전면 개편하면서 기존 테이블 스크래핑이 깨져서, 화면이 실제로 쓰는 새 JSON API로 교체함.
    """
    res = requests.get(
        "https://m.stock.naver.com/api/stocks/theme",
        headers=_naver_stock_headers(),
        params={"page": 1, "pageSize": limit},
        timeout=10,
    )
    res.raise_for_status()
    data = res.json()
    groups = data.get("groups", [])
    if not groups:
        raise ValueError("네이버에서 테마 목록을 가져오지 못했어요 — 응답 구조가 바뀌었을 수 있어요.")

    rows = []
    for i, g in enumerate(groups[:limit], start=1):
        rows.append(
            {
                "rank": i,
                "theme_no": str(g.get("no")),
                "theme_name": g.get("name", ""),
                "change_pct": float(g.get("changeRate", 0) or 0),
            }
        )
    return rows


def fetch_naver_theme_detail(theme_no: str, limit: int = 30):
    """
    테마 하나를 클릭했을 때 보여줄 구성 종목 — 마찬가지로 새 JSON API 사용.
    """
    res = requests.get(
        f"https://m.stock.naver.com/api/stocks/theme/{theme_no}",
        headers=_naver_stock_headers(),
        params={"page": 1, "pageSize": limit},
        timeout=10,
    )
    res.raise_for_status()
    data = res.json()
    stocks = data.get("stocks", [])
    if not stocks:
        raise ValueError("이 테마의 구성종목을 가져오지 못했어요 — 응답 구조가 바뀌었을 수 있어요.")

    etf_codes = _get_krx_etf_codes()

    def to_f(s):
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0

    rows = []
    for s in stocks:
        code = s.get("itemCode", "")
        name = s.get("stockName", "")
        if code in etf_codes or _is_preferred_stock_name(name):
            continue
        rows.append(
            {
                "rank": len(rows) + 1,
                "stock_code": code,
                "stock_name": name,
                "price": to_f(s.get("closePriceRaw")),
                "change_pct": to_f(s.get("fluctuationsRatio")),
                "volume": to_f(s.get("accumulatedTradingVolumeRaw")),
                "trading_value": to_f(s.get("accumulatedTradingValueRaw")),
            }
        )
        if len(rows) >= limit:
            break

    col_names = ["종목명", "현재가", "등락률", "거래량", "거래대금"]
    return rows, col_names


def fetch_naver_nxt_ranking(limit: int = 30):
    """
    네이버 증권 "NXT 거래상위"(https://finance.naver.com/sise/nxt_sise_quant.naver) 스크래핑.
    ⚠️ 공식 API가 아니라 페이지를 직접 긁는 방식이라, 네이버가 페이지 구조를 바꾸면
    깨질 수 있음 — 컬럼 순서를 하드코딩하지 않고 <thead>의 <th> 텍스트를 그대로 읽어서
    "이 값이 어느 컬럼인지" 런타임에 판단하게 만들어서, 순서가 조금 바뀌는 정도는
    안 깨지게 방어함. 완전히 다른 구조로 바뀌면(예: JS 렌더링 방식으로 전환) 그때는
    다시 손봐야 함.
    """
    if BeautifulSoup is None:
        raise RuntimeError("beautifulsoup4가 설치되어 있지 않습니다 (requirements.txt 확인 필요).")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }
    res = requests.get("https://finance.naver.com/sise/nxt_sise_quant.naver", headers=headers, timeout=10)
    soup = BeautifulSoup(res.content.decode("euc-kr", errors="replace"), "html.parser")

    table = soup.find("table", class_="type_2")
    if table is None:
        raise ValueError("페이지에서 종목 테이블(table.type_2)을 찾지 못했어요 — 네이버 쪽 구조가 바뀌었을 수 있어요.")

    # 헤더 컬럼명을 그대로 읽어서 순서 매핑 (하드코딩 안 함)
    header_ths = table.find("thead").find_all("th")
    col_names = [th.get_text(strip=True) for th in header_ths]

    etf_codes = _get_krx_etf_codes()  # ETF 제외용

    rows = []
    trs = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")
    rank = 0
    for tr in trs:
        link = tr.find("a", href=re.compile(r"code=\d{6}"))
        if link is None:
            continue  # 광고/빈 줄/구분선 행은 건너뜀
        code_match = re.search(r"code=(\d{6})", link["href"])
        if not code_match:
            continue
        code = code_match.group(1)
        name = link.get_text(strip=True)

        if code in etf_codes or _is_preferred_stock_name(name):
            continue  # ETF/우선주 제외 — 순위(rank)엔 안 넣고 그냥 건너뜀

        tds = tr.find_all("td")
        cell_texts = [td.get_text(strip=True) for td in tds]
        # cell_texts[0]은 순번, cell_texts[1]은 종목명 링크 — 그 뒤부터 헤더와 대응
        value_map = {}
        for i, col in enumerate(col_names[2:], start=2):
            if i < len(cell_texts):
                value_map[col] = cell_texts[i]

        def to_num(s):
            if not s:
                return 0.0
            s = s.replace(",", "").replace("%", "").replace("+", "")
            try:
                return float(s)
            except ValueError:
                return 0.0

        rank += 1
        rows.append(
            {
                "rank": rank,
                "stock_code": code,
                "stock_name": name,
                "price": to_num(value_map.get("현재가", "0")),
                "change_pct": to_num(value_map.get("등락률", "0")),
                "volume": to_num(value_map.get("거래량", "0")),
                "trading_value": to_num(value_map.get("거래대금", "0")),  # 네이버 표시 단위 그대로(백만원)
            }
        )
        if len(rows) >= limit:
            break

    if not rows:
        raise ValueError("테이블은 찾았는데 종목 행을 하나도 못 뽑았어요 — 컬럼 구조를 다시 확인해야 해요.")
    return rows, col_names


def fetch_overseas_group_ranking(markets: list, limit: int = 30):
    """
    "실시간 랭킹" 해외 컬럼용 — 그룹 안의 거래소 여러 개(예: 미국=NAS+NYS+AMS)를 각각
    조회해서 하나로 합친 뒤 거래량(tvol) 기준으로 다시 정렬. HHDFS76310010(해외주식
    거래량순위, 2026-08-08 필드 확인 완료: rsym/excd/symb/name/last/rate/tvol/tamt 등).
    ETF/우선주/워런트는 제외 — "종목"만 순수하게 보고 싶다는 요청 반영.
    ⚠️ 해외는 국내처럼 확실한 판별 필드가 없어서, 종목명에 흔한 키워드(ETF/FUND/TRUST 등)가
    있는지로 걸러냄 — 100% 정확하진 않지만 실무에서 흔히 쓰는 방식.
    """

    def to_f(s):
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0

    exclude_keywords = ("ETF", "FUND", "TRUST", "INDEX", "PREFERRED", "WARRANT", "NOTES", " SHARES", "PROSHARES")

    def is_fund_or_warrant(symbol: str, name: str) -> bool:
        name_upper = (name or "").upper()
        if any(kw in name_upper for kw in exclude_keywords):
            return True
        if symbol and (symbol.endswith(".W") or "-P" in symbol or ".PR" in symbol):
            return True  # 워런트/우선주 심볼 패턴
        return False

    all_rows = []
    for market in markets:
        try:
            data = kis_get(
                "/uapi/overseas-stock/v1/ranking/trade-vol",
                "HHDFS76310010",
                {"KEYB": "", "AUTH": "", "EXCD": market, "NDAY": "0", "PRC1": "", "PRC2": "", "VOL_RANG": "0"},
            )
            for r in data.get("output2", []):
                symbol = r.get("symb") or ""
                name = r.get("name") or r.get("ename") or symbol
                if is_fund_or_warrant(symbol, name):
                    continue
                all_rows.append(
                    {
                        "market": market,
                        "symbol": symbol,
                        "stock_name": name,
                        "price": to_f(r.get("last")),
                        "change_pct": to_f(r.get("rate")),
                        "volume": to_f(r.get("tvol")),
                        "trading_value": to_f(r.get("tamt")),
                    }
                )
        except Exception as e:
            print(f"[overseas_ranking] {market} 조회 실패: {e}")

    all_rows.sort(key=lambda r: r["volume"], reverse=True)
    top = all_rows[:limit]
    for i, r in enumerate(top, start=1):
        r["rank"] = i
    return top


# 업종분석 해외 탭용 — 그룹당 대표 거래소 1개로만 업종을 집계함 (그룹 안 거래소 전부 다
# 합치면 업종 40개 x 거래소 3개 = 최대 120번 API 호출이라 너무 느려짐. 대표 거래소
# 하나만 써도 그 그룹의 업종 분위기를 보는 용도로는 충분하다고 판단함)
OVERSEAS_SECTOR_PRIMARY_MARKET = {"US": "NAS", "JP": "TSE", "HK_CN": "HKS"}


def fetch_overseas_sector_list(group_name: str, limit: int = 25):
    """
    "업종분석" 해외 탭 — 업종별시세(HHDFS76370000)가 "업종 평균 등락률"을 직접 안 줘서,
    업종코드조회(HHDFS76370100)로 업종 목록을 받은 다음, 업종마다 구성종목을 다시 불러와서
    그 안의 등락률(rate)을 저희가 직접 평균 내서 계산함.
    """
    market = OVERSEAS_SECTOR_PRIMARY_MARKET.get(group_name)
    if not market:
        raise ValueError(f"알 수 없는 그룹: {group_name}")

    code_data = kis_get(
        "/uapi/overseas-price/v1/quotations/industry-price", "HHDFS76370100", {"AUTH": "", "EXCD": market}
    )
    industries = [i for i in code_data.get("output2", []) if i.get("icod") and i.get("icod") != "000"]

    results = []
    for ind in industries:
        icod = ind.get("icod")
        name = ind.get("name")
        try:
            detail = kis_get(
                "/uapi/overseas-price/v1/quotations/industry-theme",
                "HHDFS76370000",
                {"KEYB": "", "AUTH": "", "EXCD": market, "ICOD": icod, "VOL_RANG": "0"},
            )
            stocks = detail.get("output2") or []
            rates = []
            for s in stocks:
                try:
                    rates.append(float(s.get("rate") or 0))
                except (TypeError, ValueError):
                    continue
            if not rates:
                continue
            avg_change = sum(rates) / len(rates)
            results.append(
                {"icod": icod, "sector_name": name, "change_pct": round(avg_change, 2), "stock_count": len(rates)}
            )
        except Exception as e:
            print(f"[overseas_sector:{group_name}] {icod}({name}) 조회 실패: {e}")

    results.sort(key=lambda r: r["change_pct"], reverse=True)
    top = results[:limit]
    for i, r in enumerate(top, start=1):
        r["rank"] = i
    return top


def fetch_overseas_sector_detail(group_name: str, icod: str, limit: int = 30):
    """업종 하나를 클릭했을 때 보여줄 구성 종목 — ETF/우선주/워런트 제외는 거래량순위와 동일 로직 재사용"""
    market = OVERSEAS_SECTOR_PRIMARY_MARKET.get(group_name)
    if not market:
        raise ValueError(f"알 수 없는 그룹: {group_name}")

    def to_f(s):
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0

    exclude_keywords = ("ETF", "FUND", "TRUST", "INDEX", "PREFERRED", "WARRANT", "NOTES", " SHARES", "PROSHARES")

    def is_fund_or_warrant(symbol: str, name: str) -> bool:
        name_upper = (name or "").upper()
        if any(kw in name_upper for kw in exclude_keywords):
            return True
        if symbol and (symbol.endswith(".W") or "-P" in symbol or ".PR" in symbol):
            return True
        return False

    data = kis_get(
        "/uapi/overseas-price/v1/quotations/industry-theme",
        "HHDFS76370000",
        {"KEYB": "", "AUTH": "", "EXCD": market, "ICOD": icod, "VOL_RANG": "0"},
    )
    rows = []
    for r in data.get("output2") or []:
        symbol = r.get("symb") or ""
        name = r.get("name") or r.get("ename") or symbol
        if is_fund_or_warrant(symbol, name):
            continue
        rows.append(
            {
                "rank": len(rows) + 1,
                "symbol": symbol,
                "market": market,
                "stock_name": name,
                "price": to_f(r.get("last")),
                "change_pct": to_f(r.get("rate")),
                "volume": to_f(r.get("tvol")),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _ensure_overseas_ranking_cached(group_name: str):
    """
    해외 거래량 상위 마감 스냅샷 — 그룹(US/JP/HK_CN)별로 각자의 마감 시각(KST) 기준
    하루 1번만 저장. OVERSEAS_RANKING_GROUPS에 그룹별 거래소 목록/마감 시각이 정의돼있음.
    """
    group = OVERSEAS_RANKING_GROUPS.get(group_name)
    if not group:
        return
    now = now_kst()
    if now.weekday() >= 5:
        return
    scan_time = now.replace(hour=group["hour"], minute=group["minute"], second=0, microsecond=0)
    if now < scan_time:
        return

    trade_date = now.strftime("%Y-%m-%d")  # 해외는 한투 API가 이미 그날 최종치를 주는 거라 달력 날짜 그대로 씀

    try:
        existing = (
            supabase.table("overseas_daily_ranking")
            .select("symbol")
            .eq("scan_date", trade_date)
            .eq("group_name", group_name)
            .limit(1)
            .execute()
        )
        if existing.data:
            return
    except Exception as e:
        print(f"[overseas_ranking:{group_name}] 캐시 확인 실패: {e}")
        return

    try:
        rows = fetch_overseas_group_ranking(group["markets"], limit=30)
        for r in rows:
            r["scan_date"] = trade_date
            r["group_name"] = group_name
        supabase.table("overseas_daily_ranking").delete().eq("scan_date", trade_date).eq(
            "group_name", group_name
        ).execute()
        supabase.table("overseas_daily_ranking").insert(rows).execute()
        print(f"[overseas_ranking:{group_name}] {trade_date} 스캔 완료, {len(rows)}종목 캐싱")
    except Exception as e:
        print(f"[overseas_ranking:{group_name}] 스캔 실패: {e}")


def _ensure_naver_nxt_ranking_cached():
    """네이버 NXT 거래상위 마감 스냅샷 — NXT 마감(20:00) 직후 하루 1번만 저장"""
    now = now_kst()
    if now.weekday() >= 5:
        return
    scan_time = now.replace(hour=NAVER_NXT_SCAN_HOUR, minute=NAVER_NXT_SCAN_MINUTE, second=0, microsecond=0)
    if now < scan_time:
        return

    try:
        trade_date = _resolve_nxt_scan_trade_date()
    except Exception as e:
        print(f"[naver_nxt_ranking] 거래일 확인 실패: {e}")
        return

    try:
        existing = (
            supabase.table("naver_nxt_daily_ranking").select("stock_code").eq("scan_date", trade_date).limit(1).execute()
        )
        if existing.data:
            return
    except Exception as e:
        print(f"[naver_nxt_ranking] 캐시 확인 실패: {e}")
        return

    try:
        rows, _ = fetch_naver_nxt_ranking(limit=30)
        for r in rows:
            r["scan_date"] = trade_date
        supabase.table("naver_nxt_daily_ranking").delete().eq("scan_date", trade_date).execute()
        supabase.table("naver_nxt_daily_ranking").insert(rows).execute()
        print(f"[naver_nxt_ranking] {trade_date} 스캔 완료, {len(rows)}종목 캐싱")
    except Exception as e:
        print(f"[naver_nxt_ranking] 스캔 실패: {e}")


def fetch_krx_volume_ranking(limit: int = 30):
    """
    "실시간 랭킹" 페이지용 — KRX 전체(코스피+코스닥)에서 거래량(체결 수량) 기준 상위.
    fetch_volume_top30()은 거래대금 기준이라 별개 함수로 둠 — 사부님 사이트의
    "거래량 상위" 컬럼과 맞추기 위해 정렬 기준만 거래량으로 다르게 함.
    ETF/우선주는 제외 — "종목"만 순수하게 보고 싶다는 요청 반영.
    """
    ymd = _latest_trading_day()
    df = _krx_df(stock.get_market_ohlcv_by_ticker, ymd, market="ALL")
    if df.empty:
        raise ValueError(f"KRX에서 {ymd} 시세 데이터를 가져오지 못했습니다.")

    etf_codes = _get_krx_etf_codes()
    df = df.sort_values("거래량", ascending=False)
    trade_date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"

    rows = []
    for code, row in df.iterrows():
        if code in etf_codes:
            continue
        name = _safe_ticker_name(code)
        if _is_preferred_stock_name(name):
            continue
        rows.append(
            {
                "scan_date": trade_date,
                "rank": len(rows) + 1,
                "stock_code": code,
                "stock_name": name,
                "volume": int(row.get("거래량", 0)),
                "trading_value": round(float(row.get("거래대금", 0)) / 1e8, 1),  # 억원
                "change_pct": float(row.get("등락률", 0)),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _ensure_krx_ranking_cached():
    """KRX 거래량 상위도 NXT랑 똑같은 방식(15:35 기준 하루 1번)으로 마감 스냅샷 저장"""
    now = now_kst()
    if now.weekday() >= 5:
        return
    scan_time = now.replace(hour=NXT_SCAN_HOUR, minute=NXT_SCAN_MINUTE, second=0, microsecond=0)
    if now < scan_time:
        return

    try:
        trade_date = _resolve_nxt_scan_trade_date()  # KRX/NXT 둘 다 같은 거래일 기준이라 그대로 재사용
    except Exception as e:
        print(f"[krx_ranking] 거래일 확인 실패: {e}")
        return

    try:
        existing = (
            supabase.table("krx_daily_ranking").select("stock_code").eq("scan_date", trade_date).limit(1).execute()
        )
        if existing.data:
            return
    except Exception as e:
        print(f"[krx_ranking] 캐시 확인 실패: {e}")
        return

    try:
        rows = fetch_krx_volume_ranking(limit=30)
        supabase.table("krx_daily_ranking").delete().eq("scan_date", trade_date).execute()
        supabase.table("krx_daily_ranking").insert(rows).execute()
        print(f"[krx_ranking] {trade_date} 스캔 완료, {len(rows)}종목 캐싱")
    except Exception as e:
        print(f"[krx_ranking] 스캔 실패: {e}")


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


@app.get("/api/naver-theme-detail-debug")
def naver_theme_detail_debug(theme_no: str = "449"):
    if BeautifulSoup is None:
        return {"ok": False, "error": "beautifulsoup4가 설치되어 있지 않습니다."}
    try:
        url = f"https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={theme_no}"
        res = requests.get(url, headers=_naver_headers(), timeout=10)
        raw_text_preview = res.content.decode("euc-kr", errors="replace")[:1500]
        soup = BeautifulSoup(res.content.decode("euc-kr", errors="replace"), "html.parser")

        all_tables = soup.find_all("table")
        table_info = [{"index": i, "class": t.get("class")} for i, t in enumerate(all_tables)]

        table = soup.find("table", class_="type_5")
        result = {
            "ok": True,
            "status_code": res.status_code,
            "final_url": res.url,
            "requested_url": url,
            "content_length": len(res.content),
            "raw_text_preview": raw_text_preview,
            "all_tables": table_info,
            "type_5_found": table is not None,
        }
        if table:
            ths = table.find_all("th")
            result["header_texts"] = [th.get_text(strip=True) for th in ths]
            trs = table.find_all("tr")[:5]
            rows_preview = []
            for tr in trs:
                tds = tr.find_all("td")
                rows_preview.append([td.get_text(strip=True) for td in tds])
            result["rows_preview"] = rows_preview
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}



@app.get("/api/kis-overseas-industry-code-debug")
def kis_overseas_industry_code_debug(market: str = "NAS"):
    """디버그 전용 — 해외주식 업종별코드조회(HHDFS76370100) 원본 응답 확인 (업종 목록 + ICOD)"""
    try:
        data = kis_get(
            "/uapi/overseas-price/v1/quotations/industry-price",
            "HHDFS76370100",
            {"AUTH": "", "EXCD": market},
        )
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/kis-overseas-industry-theme-debug")
def kis_overseas_industry_theme_debug(market: str = "NAS", icod: str = ""):
    """디버그 전용 — 해외주식 업종별시세(HHDFS76370000) 원본 응답 확인 (업종 하나의 구성종목/등락률)"""
    try:
        data = kis_get(
            "/uapi/overseas-price/v1/quotations/industry-theme",
            "HHDFS76370000",
            {"KEYB": "", "AUTH": "", "EXCD": market, "ICOD": icod, "VOL_RANG": "0"},
        )
        return {"ok": True, "raw": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/naver-theme-list-debug")
def naver_theme_list_debug():
    """
    디버그 전용 — 네이버 증권 "테마별 시세" 목록 페이지 구조 확인.
    테마명, 등락률, 상세페이지로 가는 링크(no=XXX 파라미터)를 어떻게 뽑아낼지
    확인하려는 용도.
    """
    if BeautifulSoup is None:
        return {"ok": False, "error": "beautifulsoup4가 설치되어 있지 않습니다."}
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://finance.naver.com/",
            "Accept-Language": "ko-KR,ko;q=0.9",
        }
        res = requests.get("https://finance.naver.com/sise/theme.naver", headers=headers, timeout=10)
        soup = BeautifulSoup(res.content.decode("euc-kr", errors="replace"), "html.parser")

        tables = [t.get("class") for t in soup.find_all("table")]
        theme_links = soup.find_all("a", href=re.compile(r"sise_group_detail\.naver"))
        sample_links = [{"text": a.get_text(strip=True), "href": a["href"]} for a in theme_links[:10]]

        table = soup.find("table", class_="type_1")
        rows_preview = []
        if table:
            trs = table.find_all("tr")[:8]
            for tr in trs:
                tds = tr.find_all("td")
                rows_preview.append([td.get_text(strip=True) for td in tds])

        return {
            "ok": True,
            "status_code": res.status_code,
            "table_classes_found": tables,
            "theme_link_count": len(theme_links),
            "sample_theme_links": sample_links,
            "type_1_table_rows_preview": rows_preview,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/naver-nxt-debug")
def naver_nxt_debug():
    """
    디버그 전용 — fetch_naver_nxt_ranking()이 실제로 파싱한 결과를 그대로 반환.
    (1차: 원본 HTML 확인 → 구조 파악 완료. 2차: 이제 실제 파싱 함수가 정확히
    뽑아내는지 구조화된 JSON으로 확인하는 단계)
    """
    try:
        rows, col_names = fetch_naver_nxt_ranking(limit=10)
        return {"ok": True, "헤더에서_읽은_컬럼명": col_names, "파싱된_상위10개": rows}
    except Exception as e:
        return {"ok": False, "error": str(e)}



@app.get("/api/kis-overseas-volume-rank-debug")
def kis_overseas_volume_rank_debug(market: str = "NAS", nday: str = "0"):
    """
    디버그 전용 — 해외주식 거래량순위(HHDFS76310010) 원본 응답을 그대로 반환.
    "실시간 랭킹" 페이지의 해외 컬럼 만들기 전에 output 필드명이 정확히 뭔지 확인하려는 용도.
    market: NAS/NYS/AMS/TSE/HKS/SHS/SZS/HSX/HNX
    nday: 0(당일)/1(2일전)/2(3일전)/3(5일전)/4(10일전)/5(20일전)/6(30일전)/7(60일전)/8(120일전)/9(1년전)
    """
    try:
        data = kis_get(
            "/uapi/overseas-stock/v1/ranking/trade-vol",
            "HHDFS76310010",
            {
                "KEYB": "",
                "AUTH": "",
                "EXCD": market,
                "NDAY": nday,
                "PRC1": "",
                "PRC2": "",
                "VOL_RANG": "0",
            },
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


@app.get("/api/sync-krx-ranking")
def sync_krx_ranking_endpoint(limit: int = 30, force: bool = False):
    """KRX 거래량 상위 수동 스캔 트리거 (디버그/테스트용) — /api/sync-nxt-ranking과 동일한 방식"""
    try:
        trade_date = _resolve_nxt_scan_trade_date()
        if not force:
            existing = (
                supabase.table("krx_daily_ranking").select("stock_code").eq("scan_date", trade_date).limit(1).execute()
            )
            if existing.data:
                return {
                    "ok": True,
                    "skipped": True,
                    "scan_date": trade_date,
                    "message": "이 거래일 스캔이 이미 있습니다 (force=true로 재스캔 가능)",
                }

        rows = fetch_krx_volume_ranking(limit=limit)
        supabase.table("krx_daily_ranking").delete().eq("scan_date", trade_date).execute()
        supabase.table("krx_daily_ranking").insert(rows).execute()
        return {"ok": True, "synced": len(rows), "scan_date": trade_date}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/krx-ranking")
def krx_ranking_endpoint(date: str = ""):
    """KRX 거래량 상위 조회 — date 비우면 가장 최근 스캔 (nxt-ranking과 동일한 방식)"""
    try:
        if date:
            trade_date = date
        else:
            latest = (
                supabase.table("krx_daily_ranking").select("scan_date").order("scan_date", desc=True).limit(1).execute()
            )
            trade_date = latest.data[0]["scan_date"] if latest.data else now_kst().strftime("%Y-%m-%d")

        res = supabase.table("krx_daily_ranking").select("*").eq("scan_date", trade_date).order("rank").execute()
        return {"ok": True, "scan_date": trade_date, "items": res.data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-naver-nxt-ranking")
def sync_naver_nxt_ranking_endpoint(force: bool = False):
    """네이버 NXT 거래상위 수동 스캔 트리거 (디버그/테스트용)"""
    try:
        trade_date = _resolve_nxt_scan_trade_date()
        if not force:
            existing = (
                supabase.table("naver_nxt_daily_ranking")
                .select("stock_code")
                .eq("scan_date", trade_date)
                .limit(1)
                .execute()
            )
            if existing.data:
                return {
                    "ok": True,
                    "skipped": True,
                    "scan_date": trade_date,
                    "message": "이 거래일 스캔이 이미 있습니다 (force=true로 재스캔 가능)",
                }

        rows, _ = fetch_naver_nxt_ranking(limit=30)
        for r in rows:
            r["scan_date"] = trade_date
        supabase.table("naver_nxt_daily_ranking").delete().eq("scan_date", trade_date).execute()
        supabase.table("naver_nxt_daily_ranking").insert(rows).execute()
        return {"ok": True, "synced": len(rows), "scan_date": trade_date}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/naver-nxt-ranking")
def naver_nxt_ranking_endpoint(date: str = ""):
    """네이버 NXT 거래상위 조회 — date 비우면 가장 최근 스캔"""
    try:
        if date:
            trade_date = date
        else:
            latest = (
                supabase.table("naver_nxt_daily_ranking")
                .select("scan_date")
                .order("scan_date", desc=True)
                .limit(1)
                .execute()
            )
            trade_date = latest.data[0]["scan_date"] if latest.data else now_kst().strftime("%Y-%m-%d")

        res = (
            supabase.table("naver_nxt_daily_ranking").select("*").eq("scan_date", trade_date).order("rank").execute()
        )
        return {"ok": True, "scan_date": trade_date, "items": res.data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-overseas-ranking")
def sync_overseas_ranking_endpoint(group: str = "US", force: bool = False):
    """해외 거래량 상위 수동 스캔 트리거 (디버그/테스트용). group: US/JP/HK_CN"""
    group_def = OVERSEAS_RANKING_GROUPS.get(group)
    if not group_def:
        return {"ok": False, "error": f"group은 {'/'.join(OVERSEAS_RANKING_GROUPS.keys())} 중 하나여야 해요."}
    try:
        trade_date = now_kst().strftime("%Y-%m-%d")
        if not force:
            existing = (
                supabase.table("overseas_daily_ranking")
                .select("symbol")
                .eq("scan_date", trade_date)
                .eq("group_name", group)
                .limit(1)
                .execute()
            )
            if existing.data:
                return {
                    "ok": True,
                    "skipped": True,
                    "scan_date": trade_date,
                    "message": "이 거래일 스캔이 이미 있습니다 (force=true로 재스캔 가능)",
                }

        rows = fetch_overseas_group_ranking(group_def["markets"], limit=30)
        for r in rows:
            r["scan_date"] = trade_date
            r["group_name"] = group
        supabase.table("overseas_daily_ranking").delete().eq("scan_date", trade_date).eq("group_name", group).execute()
        supabase.table("overseas_daily_ranking").insert(rows).execute()
        return {"ok": True, "synced": len(rows), "scan_date": trade_date, "group": group}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-ranking")
def overseas_ranking_endpoint(group: str = "US", date: str = ""):
    """해외 거래량 상위 조회 — group(US/JP/HK_CN), date 비우면 가장 최근 스캔"""
    try:
        if date:
            trade_date = date
        else:
            latest = (
                supabase.table("overseas_daily_ranking")
                .select("scan_date")
                .eq("group_name", group)
                .order("scan_date", desc=True)
                .limit(1)
                .execute()
            )
            trade_date = latest.data[0]["scan_date"] if latest.data else now_kst().strftime("%Y-%m-%d")

        res = (
            supabase.table("overseas_daily_ranking")
            .select("*")
            .eq("scan_date", trade_date)
            .eq("group_name", group)
            .order("rank")
            .execute()
        )
        return {"ok": True, "scan_date": trade_date, "group": group, "items": res.data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── 업종분석 (국내=테마, 해외=섹터) — 목록은 캐싱+새로고침, 구성종목 상세는 그때그때 실시간 조회 ──


@app.get("/api/naver-theme-list")
def naver_theme_list_endpoint():
    """국내 테마 목록 조회 (캐시된 값) — 없으면 빈 목록 반환"""
    try:
        res = supabase.table("naver_theme_ranking").select("*").order("rank").limit(25).execute()
        updated_at = res.data[0]["updated_at"] if res.data else None
        return {"ok": True, "updated_at": updated_at, "items": res.data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-naver-theme-list")
def sync_naver_theme_list_endpoint():
    """국내 테마 목록 새로고침 — 실시간으로 다시 긁어와서 캐시 갱신"""
    try:
        rows = fetch_naver_theme_list(limit=25)
        now_iso = now_kst().isoformat()
        for r in rows:
            r["updated_at"] = now_iso
        supabase.table("naver_theme_ranking").delete().neq("theme_no", "").execute()
        supabase.table("naver_theme_ranking").insert(rows).execute()
        return {"ok": True, "synced": len(rows), "updated_at": now_iso}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/naver-theme-detail")
def naver_theme_detail_endpoint(theme_no: str, theme_name: str = ""):
    """테마 하나의 구성 종목 — 실시간 직접 조회 (캐싱 안 함, 클릭할 때만 호출되니까)"""
    try:
        rows, col_names = fetch_naver_theme_detail(theme_no, limit=30)
        return {"ok": True, "theme_no": theme_no, "theme_name": theme_name, "columns": col_names, "items": rows}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-sector-list")
def overseas_sector_list_endpoint(group: str = "US"):
    """해외 업종 목록 조회 (캐시된 값) — 없으면 빈 목록 반환"""
    try:
        res = (
            supabase.table("overseas_sector_ranking")
            .select("*")
            .eq("group_name", group)
            .order("rank")
            .limit(25)
            .execute()
        )
        updated_at = res.data[0]["updated_at"] if res.data else None
        return {"ok": True, "group": group, "updated_at": updated_at, "items": res.data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-overseas-sector-list")
def sync_overseas_sector_list_endpoint(group: str = "US"):
    """
    해외 업종 목록 새로고침 — 업종 40개 각각 구성종목을 다시 불러와서 평균 등락률
    재계산. 업종 개수만큼 API를 호출해서 다른 새로고침보다 시간이 좀 걸림.
    """
    if group not in OVERSEAS_SECTOR_PRIMARY_MARKET:
        return {"ok": False, "error": f"group은 {'/'.join(OVERSEAS_SECTOR_PRIMARY_MARKET.keys())} 중 하나여야 해요."}
    try:
        rows = fetch_overseas_sector_list(group, limit=25)
        now_iso = now_kst().isoformat()
        for r in rows:
            r["group_name"] = group
            r["updated_at"] = now_iso
        supabase.table("overseas_sector_ranking").delete().eq("group_name", group).execute()
        supabase.table("overseas_sector_ranking").insert(rows).execute()
        return {"ok": True, "synced": len(rows), "group": group, "updated_at": now_iso}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/overseas-sector-detail")
def overseas_sector_detail_endpoint(group: str = "US", icod: str = "", sector_name: str = ""):
    """해외 업종 하나의 구성 종목 — 실시간 직접 조회 (캐싱 안 함)"""
    try:
        rows = fetch_overseas_sector_detail(group, icod, limit=30)
        return {"ok": True, "group": group, "icod": icod, "sector_name": sector_name, "items": rows}
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
    ws_stage = {"v": "준비"}  # 어느 단계에서 끊겼는지 로그로 남기기 위한 진단용
    while True:
        now = now_kst()
        start_t = now.replace(hour=7, minute=50, second=0, microsecond=0)
        end_t = now.replace(hour=20, minute=10, second=0, microsecond=0)
        if not (start_t <= now <= end_t) or now.weekday() >= 5:  # 주말 제외
            await asyncio.sleep(60)
            continue

        try:
            ws_stage["v"] = "승인키 발급"
            approval_key = get_ws_approval_key()
            # ⚠️ NXT 608개 전체 구독은 한투 웹소켓 실제 한도(등록 41건 = 종목 20개, KRX+NXT
            # 2건씩)를 훌쩍 넘어서 MAX SUBSCRIBE OVER로 계속 끊기는 문제가 있어 되돌림.
            # NXT 관심종목(무조건) + 일반 관심종목 + 최근 조회종목 + NXT 자동 스캔 상위로
            # 20개를 채움 — 자세한 우선순위는 _get_watchlist_codes() 참고.
            # ⚠️ 아래 두 함수는 Supabase/pykrx 네트워크 호출을 동기(블로킹) 방식으로 하기 때문에,
            # asyncio.to_thread로 별도 스레드에서 돌려야 이 작업이 진행되는 동안 다른 API 요청
            # (예: Control 페이지의 PC 조회 프록시)이 이벤트 루프에서 막히지 않음
            # ⚠️ 마감 스냅샷/재무제표/시장수급 같은 "하루 1번" 작업은 여기서 하지 않고
            # _daily_jobs_worker(5분 주기)로 분리함. 예전엔 웹소켓 재연결할 때마다 같이 돌아서,
            # 웹소켓이 10초마다 끊기는 날엔 KRX/Supabase를 10초마다 두드리고 자동 동기화
            # 재시도 횟수도 몇 분 만에 다 써버리는 문제가 있었음 (2026-10-08).
            codes = await asyncio.to_thread(_get_watchlist_codes)
            if not codes:
                print("[ws_worker] 구독할 종목이 없어 잠시 대기합니다.")
                await asyncio.sleep(300)
                continue

            ws_stage["v"] = "접속 중"
            async with websockets.connect(KIS_WS_URL, ping_interval=None) as ws:
                print(f"[ws_worker] 웹소켓 접속 성공 — {len(codes)}개 종목 구독 시작")
                sent = 0
                total_subs = len(codes) * 2
                for code in codes:
                    for tr_id in ("H0STCNT0", "H0NXCNT0"):
                        ws_stage["v"] = f"구독 {sent + 1}/{total_subs}번째({code} {tr_id})"
                        sub = {
                            "header": {"approval_key": approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                            "body": {"input": {"tr_id": tr_id, "tr_key": code}},
                        }
                        await ws.send(json.dumps(sub))
                        sent += 1
                        # 진단용: 구독할 때마다 서버 응답을 잠깐 확인해서, 서버가 거절하면(예: MAX SUBSCRIBE OVER,
                        # 잘못된 승인키) 연결이 끊기기 전에 그 이유를 로그에 남김 (2026-10-08 원인불명 끊김 추적)
                        try:
                            reply = await asyncio.wait_for(ws.recv(), timeout=0.3)
                            if reply.startswith("{"):
                                if '"PINGPONG"' in reply:
                                    await ws.send(reply)
                                elif "SUCCESS" not in reply:
                                    print(f"[ws_worker] 구독 거절 응답({code} {tr_id}): {reply[:300]}")
                            elif reply[:1] in ("0", "1"):
                                _parse_realtime_message(reply)
                        except asyncio.TimeoutError:
                            pass
                ws_stage["v"] = "수신 중"
                print(f"[ws_worker] {len(codes)}개 종목({NXT_WS_SUBSCRIBE_LIMIT}개 상한) 구독 요청 완료 (KRX+NXT)")
                _ws_status["last_subscribed"] = time.time()
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
                        # (하루 1번 도는 마감 스냅샷/동기화는 _daily_jobs_worker로 분리함)
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
            wait = _ws_backoff_seconds(consecutive_failures)
            print(
                f"[ws_worker] 연결 오류({consecutive_failures}번째, {type(e).__name__}, 끊긴 단계: {ws_stage['v']}), "
                f"{wait}초 후 재시도: {e}"
            )
            if consecutive_failures % 3 == 0:
                # 정확한 만료 시간을 몰라서 6시간으로 캐싱해뒀는데, 실제로는 더 일찍 상했을 수
                # 있음 — 연속으로 계속 실패하면 캐시된 키가 문제라고 보고 새로 발급받음.
                # ⚠️ 예전엔 3번째 이후 "매번" 재발급해서 10초마다 승인키를 새로 받았는데(하루 700번 넘게),
                # 이게 오히려 한투 쪽 제한에 걸려 계속 끊기는 악순환이 될 수 있어서 3번에 1번만 재발급함.
                print("[ws_worker] 연속 실패 — 승인키를 새로 발급받습니다.")
                invalidate_ws_approval_key("domestic")
            await asyncio.sleep(wait)


def _in_overseas_window(now: datetime) -> bool:
    """
    해외 거래소 체결이 있을 만한 시간대인지 확인 — "언제 여는지" 대신 "언제 확실히 다
    닫혀있는지"(16:00~21:00 KST)만 걸러내는 방식. 아시아 거래소(도쿄/홍콩/상해/심천/
    호치민/하노이, 대략 08:00~16:00 KST)와 미국 거래소(대략 21:00~07:00 KST)를 합치면
    이 구간 빼고는 거의 항상 어딘가는 열려있음.
    """
    h = now.hour
    return not (OVERSEAS_WS_IDLE_START_HOUR <= h < OVERSEAS_WS_IDLE_END_HOUR)


def _overseas_ws_allowed(now: datetime) -> bool:
    """
    해외 웹소켓을 지금 연결해도 되는지.
    ⚠️ 한투가 appkey 하나당 웹소켓 접속을 1개만 허용함 — 해외 워커가 낮에도 연결을 붙잡고 있으면
    국내 워커가 "ALREADY IN USE appkey"(OPSP8996)로 계속 거절당함 (2026-10-08, 국내 실시간 체결이
    하루 종일 하나도 안 쌓인 장애의 원인). 그래서 국내 실시간 시간(평일 07:50~20:10 KST)엔 해외가
    무조건 양보함. 해외 관심종목이 미국 주식이면 미국장은 한국시간 밤~새벽이라 영향 없음.
    (도쿄/홍콩 등 아시아 종목은 국내장과 시간이 겹쳐서 이 방식으로는 정밀 추적이 안 됨 — 필요해지면
    국내/해외 구독을 웹소켓 하나로 합치는 방식으로 바꿔야 함)
    또 예전엔 토요일을 통째로 쉬어서 미국 금요일장(한국 토요일 새벽)을 놓쳤는데, 토요일 07:50 전까지는 열어둠.
    """
    t = (now.hour, now.minute)
    wd = now.weekday()  # 0=월 ... 5=토, 6=일
    if wd < 5 and (7, 50) <= t <= (20, 10):
        return False  # 국내 실시간 시간 — 국내 워커에 양보
    if wd == 6:
        return False  # 일요일: 열린 거래소 없음
    if wd == 5 and t >= (7, 50):
        return False  # 토요일 아침 이후: 미국 금요일장 끝남
    return _in_overseas_window(now)


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
        if not _overseas_ws_allowed(now):
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
                    if not _overseas_ws_allowed(now2):
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
            wait = _ws_backoff_seconds(consecutive_failures)
            print(f"[overseas_ws_worker] 연결 오류({consecutive_failures}번째, {type(e).__name__}), {wait}초 후 재시도: {e}")
            if consecutive_failures % 3 == 0:
                print("[overseas_ws_worker] 연속 실패 — 승인키를 새로 발급받습니다.")
                invalidate_ws_approval_key("overseas")
            await asyncio.sleep(wait)


def _ws_backoff_seconds(failures: int) -> int:
    """연속 실패가 늘수록 재시도 간격을 늘림: 10초 → 20 → 40 → 80 → 최대 120초"""
    return min(10 * (2 ** max(0, failures - 1)), 120)


async def _daily_jobs_worker():
    """
    하루 1번 도는 작업(마감 스냅샷, 재무제표, 시장 수급)을 웹소켓과 상관없이 5분마다 확인.
    각 함수가 스스로 "지금 할 시간인지 / 오늘 이미 했는지"를 판단하니, 여기선 순서대로 부르기만 함.
    하나가 실패해도 나머지는 계속 돌도록 각각 따로 감쌈.
    """
    jobs = [
        ("nxt_ranking", _ensure_nxt_ranking_cached, ()),
        ("krx_ranking", _ensure_krx_ranking_cached, ()),
        ("naver_nxt_ranking", _ensure_naver_nxt_ranking_cached, ()),
        ("overseas_ranking_US", _ensure_overseas_ranking_cached, ("US",)),
        ("overseas_ranking_JP", _ensure_overseas_ranking_cached, ("JP",)),
        ("overseas_ranking_HK_CN", _ensure_overseas_ranking_cached, ("HK_CN",)),
        ("fundamentals", _ensure_fundamentals_synced_today, ()),
        ("market_flow", _ensure_market_flow_synced_today, ()),
        ("health_report", _ensure_health_report_sent_today, ()),
        ("telegram_briefs", _run_telegram_briefs, ()),
        ("target_reach", _check_target_reach, ()),
        ("earnings_disclosure", _check_earnings_disclosures, ()),
    ]
    await asyncio.sleep(30)  # 서버 막 켜졌을 때 다른 초기화가 먼저 끝나게 잠깐 대기
    while True:
        for name, fn, args in jobs:
            try:
                await asyncio.to_thread(fn, *args)
            except Exception as e:
                print(f"[daily_jobs] {name} 실패: {e}")
        await asyncio.sleep(300)


@app.on_event("startup")
async def _start_ws_worker():
    asyncio.create_task(_ws_worker())
    # 해외주식 실시간 체결 수집은 사용 안 함 (2026-10-08 사용자 요청) — 한투가 appkey당 실시간 연결을
    # 1개만 허용해서, 해외 연결이 켜져 있으면 국내 실시간 체결이 "ALREADY IN USE appkey"로 막힘.
    # 다시 쓰려면 Render 환경변수 OVERSEAS_WS_ENABLED=1 을 추가하면 됨 (국내장 시간엔 자동으로 양보함).
    if os.environ.get("OVERSEAS_WS_ENABLED") == "1":
        asyncio.create_task(_overseas_ws_worker())
    else:
        print("[overseas_ws_worker] 꺼져 있음 (OVERSEAS_WS_ENABLED=1 로 켤 수 있음)")
    asyncio.create_task(_daily_jobs_worker())


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

    df = _krx_df(stock.get_shorting_balance_by_date, fromdate, todate, code)
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
        df = _krx_df(stock.get_shorting_balance_by_date, fromdate, todate, code)
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
        ohlcv = _krx_df(stock.get_market_ohlcv_by_date, ymd, ymd, code)
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

    df = _krx_df(stock.get_market_trading_value_by_date, fromdate, todate, code, detail=True)

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



# ── 시장 전체 수급(전체/코스피/코스닥) — 원래 PC 프로그램(market_flow_sync.py)만 채우던
# market_flow_trend 테이블을 클라우드에서 매일 자동으로 채움 (2026-10-02 이전).
# PC 프로그램이 8/15 이후로 안 돌아서 시장분석 수급 차트가 8/14에서 멈춰 있었음.
# 단위는 기존 데이터와 같은 "억원". 기존 값과 단위가 다르면 저장하지 않고 멈추는 안전장치 포함.
MARKET_FLOW_MARKETS = ("ALL", "KOSPI", "KOSDAQ")


def _pick(row, *names):
    for n in names:
        v = row.get(n, None)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return 0.0


def _market_flow_df(fromdate: str, todate: str, market: str):
    """pykrx 시장 단위 투자자별 순매수(원). ALL이 실패하면 코스피+코스닥을 더해서 대신 씀."""
    try:
        df = _krx_df(stock.get_market_trading_value_by_date, fromdate, todate, market, detail=True)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"[market_flow] {market} 조회 실패: {e}")
    if market == "ALL":
        kospi = _market_flow_df(fromdate, todate, "KOSPI")
        kosdaq = _market_flow_df(fromdate, todate, "KOSDAQ")
        if kospi is not None and kosdaq is not None:
            return kospi.add(kosdaq, fill_value=0)
    return None


def fetch_market_flow(market: str, days: int = 20):
    if stock is None:
        raise RuntimeError("pykrx를 사용할 수 없습니다.")
    today = now_kst()
    fromdate = (today - timedelta(days=days * 2)).strftime("%Y%m%d")
    todate = today.strftime("%Y%m%d")
    df = _market_flow_df(fromdate, todate, market)
    if df is None or df.empty:
        raise ValueError(f"KRX에서 {market} 시장 수급을 가져오지 못했습니다 ({fromdate}~{todate}).")
    print(f"[market_flow] {market} rows={len(df)} columns={list(df.columns)}")
    df = df.tail(days)

    now_iso = now_kst().isoformat()
    rows = []
    for date_idx, row in df.iterrows():
        foreign = _pick(row, "외국인합계", "외국인") + (0.0 if "외국인합계" in row.index else _pick(row, "기타외국인"))
        # 기관 = 금융투자+보험+투신+사모+은행+기타금융+연기금 (HTS의 "기관합계"와 같은 기준)
        if "기관합계" in row.index:
            institution = _pick(row, "기관합계")
        else:
            institution = sum(_pick(row, c) for c in ("금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금"))
        rows.append(
            {
                "market": market,
                "trade_date": date_idx.strftime("%Y-%m-%d"),
                "foreign_amt": round(foreign / 1e8, 1),
                "institution": round(institution / 1e8, 1),
                "individual": round(_pick(row, "개인") / 1e8, 1),
                "pension": round(_pick(row, "연기금") / 1e8, 1),
                "updated_at": now_iso,
            }
        )
    return rows


def _check_market_flow_units(market: str, rows: list):
    """
    새로 계산한 값이 기존(PC 프로그램이 저장한) 값과 단위가 같은지 겹치는 날짜로 확인.
    외국인 순매수가 2배 넘게 차이나면 단위/계산 방식이 다른 것이니 저장하지 않고 에러를 냄.
    """
    dates = [r["trade_date"] for r in rows]
    try:
        old = (
            supabase.table("market_flow_trend")
            .select("trade_date,foreign_amt,individual")
            .eq("market", market)
            .in_("trade_date", dates)
            .execute()
            .data
        )
    except Exception as e:
        print(f"[market_flow] 기존값 비교 조회 실패 (비교 없이 진행): {e}")
        return
    new_by_date = {r["trade_date"]: r for r in rows}
    ratios = []
    for o in old:
        n = new_by_date.get(o["trade_date"])
        old_v = float(o.get("foreign_amt") or 0)
        if n and abs(old_v) >= 100:
            ratios.append(n["foreign_amt"] / old_v)
    if not ratios:
        return
    ratios.sort()
    median = ratios[len(ratios) // 2]
    if not (0.5 <= median <= 2.0):
        raise ValueError(
            f"{market}: 새 값과 기존 값의 크기가 너무 달라서(약 {median:.2f}배) 저장을 멈췄어요. "
            "단위나 계산 방식이 기존 PC 프로그램과 다를 수 있어요."
        )


def sync_market_flow(days: int = 20) -> dict:
    result = {}
    for market in MARKET_FLOW_MARKETS:
        try:
            rows = fetch_market_flow(market, days)
            _check_market_flow_units(market, rows)
            supabase.table("market_flow_trend").upsert(rows).execute()
            result[market] = {"ok": True, "synced": len(rows), "last_date": rows[-1]["trade_date"] if rows else None}
        except Exception as e:
            print(f"[market_flow] {market} 동기화 실패: {e}")
            result[market] = {"ok": False, "error": str(e)}
    return result


_market_flow_last_try = {"at": 0.0}


def _ensure_market_flow_synced_today():
    """
    평일 16:30~21:00 KST 사이, 오늘 날짜 행이 아직 없으면 동기화 (투자자별 집계가 장 마감 후 확정되므로).
    실패하거나 오늘 데이터가 아직 안 나왔으면 15분 뒤에 다시 시도 — 휴장일(대체공휴일 등)엔
    오늘 데이터가 끝까지 안 생기니 21시에 그날은 포기함.
    """
    now = now_kst()
    if now.weekday() >= 5 or not ((16, 30) <= (now.hour, now.minute) < (21, 0)):
        return
    if time.time() - _market_flow_last_try["at"] < 15 * 60:
        return
    today_str = now.strftime("%Y-%m-%d")
    try:
        existing = (
            supabase.table("market_flow_trend")
            .select("trade_date")
            .eq("market", "ALL")
            .eq("trade_date", today_str)
            .limit(1)
            .execute()
        )
        if existing.data:
            return
    except Exception as e:
        print(f"[market_flow] 캐시 확인 실패: {e}")
        return
    _market_flow_last_try["at"] = time.time()
    result = sync_market_flow(days=20)
    print(f"[market_flow] 자동 동기화 결과: {result}")


@app.get("/api/sync-market-flow")
def sync_market_flow_endpoint(days: int = 40):
    """
    시장 전체 수급(전체/코스피/코스닥) 수동 동기화 — 브라우저로 이 주소를 한 번 열면
    최근 days 영업일치를 채움 (기본 40일: 8월 중순 이후 비어있던 구간까지 한 번에 메움).
    평소엔 _ws_worker가 평일 16:30 이후 자동으로 돌림.
    """
    days = max(1, min(days, 120))
    return {"ok": True, "result": sync_market_flow(days)}


def _latest_trading_day() -> str:
    """오늘이 주말/공휴일이라 데이터가 없을 수 있어서, 최근 영업일을 찾아 반환 (YYYYMMDD)"""
    from datetime import date as _date

    d = now_kst()
    for _ in range(10):
        ymd = d.strftime("%Y%m%d")
        # 코스피 지수 하나로 그날 데이터가 있는지 간단히 확인
        # 첫날(오늘)만 재시도 — 세션 만료로 빈 값이 오면 어제를 "최근 영업일"로 잘못 고르는 문제 방지
        test = _krx_df(stock.get_index_ohlcv_by_date, ymd, ymd, "1001", retries=2 if _ == 0 else 1)
        if test is not None and not test.empty:
            return ymd
        d -= timedelta(days=1)
    raise ValueError("최근 10일 내 KRX 영업일을 찾지 못했습니다")


# ── 재무제표(펀더멘털) 동기화 — 원래 PC 프로그램(fundamentals_sync.py)에서만 돌리던 걸
# 클라우드로 이전. pykrx만 쓰고 키움(PC 전용) 의존성이 전혀 없어서 그대로 옮길 수 있었음
# (2026-08-09). PER/PBR/EPS/BPS는 KRX 공식 제공값, ROE는 EPS/BPS 기반 근사치 —
# pykrx가 실제 재무제표 원장을 안 줘서 완전 정확한 값은 아님(원본 스크립트와 동일한 한계).
def _calc_fundamentals_one(code: str, name: str, today: str, year_ago: str):
    try:
        fdf = _krx_df(stock.get_market_fundamental, today, today, code)
        if fdf.empty:
            print(f"[fundamentals_sync] {code} 펀더멘털 데이터 없음 (휴장일이거나 상장폐지 종목일 수 있음)")
            return None
        row = fdf.iloc[0]
        per = float(row.get("PER", 0) or 0)
        pbr = float(row.get("PBR", 0) or 0)
        eps = float(row.get("EPS", 0) or 0)
        bps = float(row.get("BPS", 0) or 0)
        roe = round((eps / bps) * 100, 2) if bps else 0.0

        cap_df = _krx_df(stock.get_market_cap, today, today, code)
        market_cap = int(cap_df.iloc[0].get("시가총액", 0)) if not cap_df.empty else 0

        ohlcv = _krx_df(stock.get_market_ohlcv_by_date, year_ago, today, code)
        high_52w = int(ohlcv["고가"].max()) if not ohlcv.empty else 0
        low_52w = int(ohlcv["저가"].min()) if not ohlcv.empty else 0

        return {
            "code": code,
            "name": name,
            "per": per,
            "pbr": pbr,
            "roe": roe,
            "eps": eps,
            "bps": bps,
            "market_cap": market_cap,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "synced_at": now_kst().isoformat(),
        }
    except Exception as e:
        print(f"[fundamentals_sync] {code} 계산 실패: {e}")
        return None


def sync_fundamentals_market_wide():
    """
    관심종목만이 아니라 KRX 전체(코스피+코스닥) 상장종목 재무제표를 한 번에 동기화.
    종목별로 한 건씩 조회하던 sync_fundamentals_all()과 다르게, pykrx의 "그날 전체
    스냅샷" 벌크 API(market="ALL")로 API 호출 2번 만에 끝냄 — 훨씬 빠름.
    ⚠️ 대신 52주 최고/최저는 종목마다 1년치 일봉을 따로 조회해야 해서 여기선 뺐음
    (2500개 넘는 종목에 다 하면 너무 오래 걸림) — 필요하면 sync_fundamentals_all()
    (관심종목 전용, 52주 포함)을 같이 돌리면 그 종목들은 채워짐.
    """
    if stock is None:
        raise RuntimeError("pykrx를 사용할 수 없어서 재무제표 동기화를 할 수 없습니다.")

    today = _latest_trading_day()
    fdf = _krx_df(stock.get_market_fundamental, today, market="ALL")
    cap_df = _krx_df(stock.get_market_cap, today, market="ALL")
    if fdf.empty:
        raise ValueError(f"{today} 기준 재무제표 데이터를 가져오지 못했습니다.")

    synced_at = now_kst().isoformat()
    rows = []
    for code, row in fdf.iterrows():
        try:
            per = float(row.get("PER", 0) or 0)
            pbr = float(row.get("PBR", 0) or 0)
            eps = float(row.get("EPS", 0) or 0)
            bps = float(row.get("BPS", 0) or 0)
            roe = round((eps / bps) * 100, 2) if bps else 0.0
            market_cap = int(cap_df.loc[code, "시가총액"]) if code in cap_df.index else 0
            name = _safe_ticker_name(code, allow_remote=False)  # 2500종목이라 네이버 개별조회는 안 함
            rows.append(
                {
                    "code": code,
                    "name": name,
                    "per": per,
                    "pbr": pbr,
                    "roe": roe,
                    "eps": eps,
                    "bps": bps,
                    "market_cap": market_cap,
                    "high_52w": 0,  # 벌크 경로에선 계산 안 함 (아래 docstring 참고)
                    "low_52w": 0,
                    "synced_at": synced_at,
                }
            )
        except Exception as e:
            print(f"[fundamentals_sync_market] {code} 처리 실패: {e}")

    ok, fail = 0, 0
    batch_size = 200  # Supabase에 한 번에 너무 많이 보내면 문제 생길 수 있어 나눠서 전송
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            supabase.table("stock_fundamentals").upsert(batch).execute()
            ok += len(batch)
        except Exception as e:
            print(f"[fundamentals_sync_market] batch({i}~{i+len(batch)}) 저장 실패: {e}")
            fail += len(batch)

    return {"success": ok, "failed": fail, "total": len(rows)}


def sync_fundamentals_all():
    """관심종목(watchlist) 테이블 전체를 훑어서 stock_fundamentals에 upsert. 반환: {success, failed}"""
    if stock is None:
        raise RuntimeError("pykrx를 사용할 수 없어서 재무제표 동기화를 할 수 없습니다.")

    watchlist = supabase.table("watchlist").select("code,name").execute().data
    today = _latest_trading_day()
    year_ago = (now_kst() - timedelta(days=365)).strftime("%Y%m%d")

    ok, fail = 0, 0
    for row in watchlist:
        code = row.get("code")
        name = row.get("name", code)
        if not code:
            fail += 1
            continue

        result = _calc_fundamentals_one(code, name, today, year_ago)
        if result:
            try:
                supabase.table("stock_fundamentals").upsert(result).execute()
                ok += 1
            except Exception as e:
                print(f"[fundamentals_sync] {code} 저장 실패: {e}")
                fail += 1
        else:
            fail += 1

        time.sleep(0.2)  # KRX 서버 부담 줄이기

    return {"success": ok, "failed": fail}


def _ensure_fundamentals_synced_today():
    """
    하루 1번만 자동 동기화 — 국내 워커의 다른 마감 스냅샷들이랑 같은 방식(16:00 KST 이후,
    오늘자로 이미 돌았으면 스킵). PC 없이도 매일 자동으로 채워지게 하려는 용도.
    """
    now = now_kst()
    if now.weekday() >= 5:
        return
    if now.hour < 16:
        return
    try:
        today_str = now.strftime("%Y-%m-%d")
        existing = (
            supabase.table("stock_fundamentals")
            .select("code")
            .gte("synced_at", f"{today_str}T00:00:00")
            .limit(1)
            .execute()
        )
        if existing.data:
            return
    except Exception as e:
        print(f"[fundamentals_sync] 캐시 확인 실패: {e}")
        return

    try:
        result = sync_fundamentals_market_wide()
        print(
            f"[fundamentals_sync] 자동 동기화(전체 시장) 완료: 성공 {result['success']}건, "
            f"실패 {result['failed']}건, 총 {result['total']}종목"
        )
    except Exception as e:
        print(f"[fundamentals_sync] 자동 동기화 실패: {e}")


def fetch_volume_top30(limit: int = 30):
    ymd = _latest_trading_day()

    df = _krx_df(stock.get_market_ohlcv_by_ticker, ymd, market="ALL")
    print(f"[volume_top30] date={ymd} rows={len(df)} columns={list(df.columns)}")

    if df.empty:
        raise ValueError(f"KRX에서 {ymd} 시세 데이터를 가져오지 못했습니다.")

    df = df.sort_values("거래대금", ascending=False).head(limit)

    rows = []
    for i, (code, row) in enumerate(df.iterrows(), start=1):
        name = _safe_ticker_name(code)
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

    df = _krx_df(stock.get_market_trading_value_by_date, fromdate, todate, code, detail=True)
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


@app.get("/api/sync-fundamentals")
def sync_fundamentals_endpoint():
    """
    관심종목 재무제표(PER/PBR/ROE/EPS/BPS/시총/52주 고저 포함) 수동 동기화.
    52주 고저까지 필요한 관심종목만 정밀하게 채우고 싶을 때 사용 — 전체 시장은
    /api/sync-fundamentals-market 참고.
    """
    try:
        result = sync_fundamentals_all()
        return {"ok": True, **result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-fundamentals-market")
def sync_fundamentals_market_endpoint():
    """
    KRX 전체(코스피+코스닥) 상장종목 재무제표 일괄 동기화 — 관심종목 여부와 무관하게
    아무 종목이나 재무제표가 나오게 하려는 용도. 매일 16:00 KST 이후 자동으로도 돌지만,
    지금 바로 채우고 싶을 때 이걸로 수동 실행 가능. 52주 고저는 포함 안 됨(느려져서 뺌).
    """
    try:
        result = sync_fundamentals_market_wide()
        return {"ok": True, **result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 📑 증권사 리포트 (애널리스트 종목분석 리포트) — 무료 공개 자료만 모아서 "원문 링크"로 연결
# ──────────────────────────────────────────────────────────────────────────
# 출처: 네이버 증권 리서치 > 종목분석 리포트 (증권사들이 무료로 공개한 리포트 목록).
# ⚠️ 리포트 PDF를 서버에 복사·저장하지 않고 원문 링크만 보여줌 (저작권 때문).
# ⚠️ 네이버가 2026-09에 사이트를 개편해서, 옛 HTML 페이지가 안 되면 새 JSON API를 시도함.
#    둘 다 응답 모양을 이 작업 환경에서 직접 확인하지 못해서, 실패하면 /api/stock-reports-debug로
#    실제 응답을 보고 맞춰야 함.
# ══════════════════════════════════════════════════════════════════════════
from concurrent.futures import ThreadPoolExecutor

_report_cache: dict = {}  # code -> (저장시각, 결과)  ⚠️ 서버 재시작하면 비워짐
_REPORT_CACHE_SEC = 30 * 60


def _naver_research_list_html(code: str, page: int = 1) -> str:
    res = requests.get(
        "https://finance.naver.com/research/company_list.naver",
        headers=_naver_headers(),
        params={"searchType": "itemCode", "itemCode": code, "page": page},
        timeout=10,
    )
    res.raise_for_status()
    return _decode_html(res)


def _parse_naver_research_list(html: str) -> list:
    if BeautifulSoup is None:
        raise RuntimeError("beautifulsoup4가 설치되어 있지 않습니다.")
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.find_all("tr"):
        link = tr.find("a", href=re.compile(r"company_read\.naver"))
        if link is None:
            continue
        tds = tr.find_all("td")
        texts = [td.get_text(" ", strip=True) for td in tds]
        title = link.get_text(" ", strip=True)
        href = link["href"]
        detail_url = href if href.startswith("http") else "https://finance.naver.com/research/" + href.lstrip("/")
        pdf = tr.find("a", href=re.compile(r"\.pdf", re.I))
        date_txt = next((t for t in texts if re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", t)), "")
        date_iso = f"20{date_txt[0:2]}-{date_txt[3:5]}-{date_txt[6:8]}" if date_txt else ""
        # 증권사 = 제목 바로 다음 칸 (종목명 / 제목 / 증권사 / 첨부 / 작성일 / 조회수 순서)
        broker = ""
        for i, td in enumerate(tds):
            if link in td.find_all("a"):
                if i + 1 < len(texts):
                    broker = texts[i + 1]
                break
        rows.append(
            {
                "date": date_iso,
                "title": title,
                "broker": broker,
                "detail_url": detail_url,
                "pdf_url": pdf["href"] if pdf else None,
                "target_price": None,
                "opinion": None,
            }
        )
    return rows


def _decode_html(res) -> str:
    """네이버 옛 페이지는 EUC-KR, 개편된 새 페이지는 UTF-8 → 응답에 맞게 골라서 읽음"""
    head = res.content[:3000].lower()
    ctype = (res.headers.get("content-type") or "").lower()
    if "utf-8" in ctype or b"charset=utf-8" in head or b'charset="utf-8"' in head:
        return res.content.decode("utf-8", errors="replace")
    if "euc-kr" in ctype or b"euc-kr" in head:
        return res.content.decode("euc-kr", errors="replace")
    try:
        return res.content.decode("utf-8")
    except UnicodeDecodeError:
        return res.content.decode("euc-kr", errors="replace")


def _find_in_json(obj, key_re, depth=0):
    """JSON 안을 전부 돌면서 키 이름이 key_re에 맞는 첫 값(문자열/숫자)을 찾음"""
    if depth > 8:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if re.search(key_re, str(k), re.I) and isinstance(v, (str, int, float)) and str(v).strip():
                return v
        for v in obj.values():
            found = _find_in_json(v, key_re, depth + 1)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_in_json(v, key_re, depth + 1)
            if found is not None:
                return found
    return None


def _naver_research_detail(detail_url: str) -> dict:
    """
    리포트 상세 페이지에서 목표가·투자의견·PDF 주소를 뽑음 (목록엔 이게 없어서).
    ⚠️ 2026-09 개편 후 옛 주소가 새 페이지(UTF-8, "목표주가 500,000원 / 투자의견 Buy / PDF 원문 보기")로
    넘어가는데, 예전엔 EUC-KR로만 읽고 "목표가" 글자만 찾아서 한 개도 못 뽑았음 (2026-10-09 수정).
    순서: ① 페이지에 들어있는 데이터(JSON) → ② 화면 글자에서 찾기
    """
    out = {"target_price": None, "opinion": None, "pdf_url": None}
    try:
        res = requests.get(detail_url, headers=_naver_headers(), timeout=8)
        html = _decode_html(res)

        # ① Next.js 페이지 데이터(__NEXT_DATA__ 등)에 들어있는 값
        m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
        if m:
            try:
                data = json.loads(m.group(1))
                tp = _find_in_json(data, r"^(targetPrice|goalPrice|targetStockPrice|tp)$")
                op = _find_in_json(data, r"^(opinion|investOpinion|recommendation|recomm\w*)$")
                pdf = _find_in_json(data, r"(pdf|attach|file)\w*(url|path)?$")
                if tp is not None and str(tp).replace(",", "").replace(".0", "").isdigit():
                    out["target_price"] = int(float(str(tp).replace(",", "")))
                if op:
                    out["opinion"] = str(op).strip()
                if pdf and ".pdf" in str(pdf).lower():
                    out["pdf_url"] = str(pdf)
            except Exception:
                pass

        # PDF 링크 (화면에 있는 a 태그)
        if not out["pdf_url"]:
            pm = re.search(r'href="([^"]+\.pdf[^"]*)"', html, re.I) or re.search(r'(https?://[^"\'\s]+\.pdf)', html, re.I)
            if pm:
                link = pm.group(1).replace("&amp;", "&")
                inner = re.search(r"[?&]url=([^&]+)", link)
                if inner:
                    from urllib.parse import unquote
                    link = unquote(inner.group(1))
                out["pdf_url"] = link if link.startswith("http") else None

        # ② 화면 글자에서 찾기 ("목표주가 500,000원", "목표가 500,000", "투자의견 Buy/매수")
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True) if BeautifulSoup is not None else html
        if out["target_price"] is None:
            tp = re.search(r"목표\s*주?\s*가\s*[:：]?\s*([\d,]{3,})", text)
            if tp:
                out["target_price"] = int(tp.group(1).replace(",", ""))
        if not out["opinion"]:
            op = re.search(
                r"투자\s*의견\s*[:：]?\s*(Strong\s?Buy|Trading\s?Buy|Not\s?Rated|Outperform|Marketperform|Underperform|"
                r"Overweight|Underweight|Neutral|Buy|Hold|Sell|NR|강력\s?매수|단기\s?매수|매수|중립|보유|매도|비중\s?확대|비중\s?축소)",
                text,
                re.I,
            )
            if op:
                out["opinion"] = op.group(1).strip()
    except Exception as e:
        print(f"[stock_reports] 상세 페이지 읽기 실패 {detail_url}: {e}")
    return out


def _naver_research_json(code: str) -> list:
    """
    1순위 경로 — 개편된 네이버 증권의 리서치 JSON (2026-10-09 디버그로 확인됨).
    /api/research/stock/{종목코드} → [{researchId, title, brokerName, writeDate, readCount, previewContent, ...}]
    ⚠️ /api/research/company 는 itemCode를 무시하고 "전체 종목" 최신 리포트를 줘서, 종목코드로 꼭 걸러야 함.
    """
    candidates = [
        (f"https://m.stock.naver.com/api/research/stock/{code}", {"pageSize": 30, "page": 1}),
        ("https://m.stock.naver.com/api/research/company", {"itemCode": code, "pageSize": 100, "page": 1}),
    ]
    for url, params in candidates:
        try:
            res = requests.get(url, headers=_naver_stock_headers(), params=params, timeout=8)
            if not res.ok:
                continue
            data = res.json()
        except Exception:
            continue
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("researches") or data.get("items") or data.get("result") or data.get("list") or []
        else:
            items = []
        rows = []
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            if it.get("itemCode") and str(it.get("itemCode")).upper() != code.upper():
                continue  # 다른 종목 리포트는 버림
            title = it.get("title") or it.get("researchTitle") or ""
            if not title:
                continue
            date = str(it.get("writeDate") or it.get("date") or it.get("regDate") or "")[:10].replace(".", "-")
            rid = it.get("researchId") or it.get("nid") or it.get("id")
            tp_raw = str(it.get("targetPrice") or "").replace(",", "")
            rows.append(
                {
                    "date": date,
                    "title": title,
                    "broker": it.get("brokerName") or it.get("brokerage") or "",
                    "research_id": rid,
                    "detail_url": it.get("endUrl") or (f"https://m.stock.naver.com/research/company/{rid}" if rid else None),
                    "pdf_url": it.get("attachUrl") or it.get("pdfUrl") or it.get("fileUrl"),
                    "target_price": int(tp_raw) if tp_raw.isdigit() else None,
                    "opinion": it.get("opinion") or it.get("investOpinion"),
                    "summary": (it.get("previewContent") or "").strip() or None,
                    "read_count": it.get("readCount"),
                }
            )
        if rows:
            return rows
    return []


def _naver_research_detail_json(research_id) -> dict:
    """
    리포트 1건의 상세 JSON — /api/research/company/{researchId} (2026-10-09 디버그로 확인됨).
    researchContent 안에 goalPrice(목표주가)·prevGoalPrice(직전 목표주가)·opinion(투자의견)·
    attachUrl(PDF)·priceAtWriteDate(작성일 주가)가 들어있음.
    """
    out = {"target_price": None, "opinion": None, "pdf_url": None, "prev_target_price": None, "price_at_write": None}
    if not research_id:
        return out

    def to_int(v):
        try:
            n = int(float(str(v).replace(",", "").replace("원", "").strip()))
            return n if n > 0 else None
        except (TypeError, ValueError):
            return None

    try:
        res = requests.get(
            f"https://m.stock.naver.com/api/research/company/{research_id}", headers=_naver_stock_headers(), timeout=6
        )
        if not res.ok:
            return out
        data = res.json()
    except Exception:
        return out
    c = data.get("researchContent") if isinstance(data, dict) else None
    if not isinstance(c, dict):
        c = {}
    out["target_price"] = to_int(c.get("goalPrice")) or to_int(_find_in_json(data, r"^(goalPrice|targetPrice)$"))
    # ⚠️ prevGoalPrice는 "직전 목표가"가 아니라 작성일 주가와 같은 값이 들어와서(2026-10-11 확인) 쓰지 않음.
    # 직전 목표가는 같은 증권사의 이전 리포트에서 직접 찾음.
    out["prev_target_price"] = None
    out["price_at_write"] = to_int(c.get("priceAtWriteDate"))
    op = c.get("opinion") or _find_in_json(data, r"^(opinion|investOpinion)$")
    out["opinion"] = str(op).strip() if op else None
    pdf = c.get("attachUrl")
    out["pdf_url"] = pdf if pdf and ".pdf" in str(pdf).lower() else None
    return out


def fetch_stock_reports(code: str, limit: int = 40) -> dict:
    cached = _report_cache.get(code)
    if cached and time.time() - cached[0] < _REPORT_CACHE_SEC:
        return cached[1]

    rows, source, errors = [], "", []
    try:
        rows = _naver_research_json(code)
        if rows:
            source = "네이버 증권 리서치"
    except Exception as e:
        errors.append(f"네이버 리서치 API: {e}")
    if not rows:
        # 예비: 개편 전 HTML 목록 (지금은 새 페이지로 바뀌어서 대부분 0건)
        try:
            rows = _parse_naver_research_list(_naver_research_list_html(code))
            if rows:
                source = "네이버 증권 리서치(옛 페이지)"
        except Exception as e:
            errors.append(f"네이버 리서치 목록: {e}")

    rows = rows[:limit]
    # 목록에 목표가가 없으면 최근 15개만 상세 페이지에서 목표가·투자의견을 채움 (동시에 6개씩)
    need = [
        r for r in rows[:40]
        if r.get("detail_url") and (r["target_price"] is None or not r.get("pdf_url") or not r.get("opinion"))
    ]
    if need:
        with ThreadPoolExecutor(max_workers=6) as ex:
            def _detail(x):
                d = _naver_research_detail_json(x.get("research_id"))
                if d["target_price"] is None or not d["pdf_url"] or not d["opinion"]:
                    h = _naver_research_detail(x["detail_url"])
                    for k, v in h.items():
                        if v and not d.get(k):
                            d[k] = v
                return d

            for r, d in zip(need, ex.map(_detail, need)):
                for k, v in d.items():
                    if v is not None and not r.get(k):
                        r[k] = v

    for i, r in enumerate(rows):
        r["prev_target_price"] = next(
            (o["target_price"] for o in rows[i + 1:] if o.get("broker") == r.get("broker") and o.get("target_price")),
            None,
        )
    result = {"ok": True, "code": code, "source": source, "items": rows}
    if not rows:
        result["note"] = "최근 리포트가 없어요. 증권사가 분석하지 않는 소형주는 리포트가 아예 없는 경우가 많아요."
        result["errors"] = errors
    _report_cache[code] = (time.time(), result)
    return result


@app.get("/api/stock-reports")
def stock_reports_endpoint(code: str, refresh: bool = False):
    """종목별 증권사 리포트 목록 (제목·증권사·날짜·목표가·투자의견·원문 링크). 30분 캐시."""
    try:
        code = (code or "").strip().upper()
        if refresh:
            _report_cache.pop(code, None)
        return fetch_stock_reports(code)
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/stock-reports-debug")
def stock_reports_debug(code: str = "005930"):
    """디버그 전용 — 리포트 출처들의 실제 응답 앞부분을 그대로 보여줌 (구조가 바뀌었을 때 맞추는 용도)"""
    out = {}
    try:
        html = _naver_research_list_html(code)
        out["naver_html_len"] = len(html)
        top = _parse_naver_research_list(html)[:3]
        out["naver_parsed_top3"] = top
        if top and top[0].get("detail_url"):
            dres = requests.get(top[0]["detail_url"], headers=_naver_headers(), timeout=8)
            out["detail_final_url"] = dres.url
            out["detail_parsed"] = _naver_research_detail(top[0]["detail_url"])
            dhtml = _decode_html(dres)
            out["detail_has_next_data"] = "__NEXT_DATA__" in dhtml
            i = dhtml.find("목표")
            out["detail_text_near_목표"] = dhtml[max(0, i - 300): i + 500] if i >= 0 else dhtml[:800]
        out["naver_html_preview"] = html[:1500]
    except Exception as e:
        out["naver_html_error"] = str(e)
    try:
        jrows = _naver_research_json(code)
        out["json_rows_top2"] = jrows[:2]
        if jrows:
            rid = jrows[0].get("research_id")
            out["detail_json_parsed"] = _naver_research_detail_json(rid)
            for durl in (f"https://m.stock.naver.com/api/research/company/{rid}",):
                try:
                    dr = requests.get(durl, headers=_naver_stock_headers(), timeout=6)
                    out[durl] = {"status": dr.status_code, "preview": dr.text[:1200]}
                except Exception as e:
                    out[durl] = {"error": str(e)}
            if jrows[0].get("detail_url"):
                out["detail_page_parsed"] = _naver_research_detail(jrows[0]["detail_url"])
    except Exception as e:
        out["json_error"] = str(e)
    for url in (f"https://m.stock.naver.com/api/research/stock/{code}",):
        try:
            r = requests.get(url, headers=_naver_stock_headers(), params={"itemCode": code, "pageSize": 5, "page": 1}, timeout=8)
            out[url] = {"status": r.status_code, "preview": r.text[:800]}
        except Exception as e:
            out[url] = {"error": str(e)}
    return out


# ══════════════════════════════════════════════════════════════════════════
# 📚 리서치 (산업분석 · 투자전략 · 경제분석 · 채권분석 · 데일리 시황) — 네이버 증권 리서치 무료 공개분
# ──────────────────────────────────────────────────────────────────────────
# 종목분석은 /api/research/stock/{코드}, /api/research/company/{id} 로 확인됐는데, 나머지 분류의
# 정확한 주소는 아직 직접 확인을 못 해서 후보를 차례로 시도하고, 처음 성공한 주소를 기억해둠.
# 안 되면 /api/research-debug 로 실제 응답을 보고 후보를 고치면 됨.
# ⚠️ PDF는 저장하지 않고 원문 링크만 연결 (저작권).
# ══════════════════════════════════════════════════════════════════════════
# 2026-10-09 /api/research-debug 로 실제 주소 확인 완료 (나머지 후보는 404였음)
RESEARCH_CATEGORIES = {
    "industry": {"label": "산업분석", "paths": ["industry"]},
    "strategy": {"label": "투자전략", "paths": ["invest"]},
    "economy": {"label": "경제분석", "paths": ["economy"]},
    "bond": {"label": "채권분석", "paths": ["debenture"]},
    "market": {"label": "데일리(시황)", "paths": ["market", "daily"]},
}
_research_path_ok: dict = {}  # category -> 실제로 동작한 경로 이름
_research_cache: dict = {}  # (category, page) -> (시각, 결과)
_RESEARCH_CACHE_SEC = 15 * 60


def _research_items_from(data) -> list:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("researches", "items", "result", "list", "researchSummaries"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    return []


def _research_row(it: dict, cat_label: str, path: str):
    if not isinstance(it, dict):
        return None
    title = it.get("title") or it.get("researchTitle")
    if not title:
        return None
    rid = it.get("researchId") or it.get("nid") or it.get("id")
    # 산업분석은 "반도체/자동차" 같은 산업 이름이 들어있는 칸이 있음 — 이름이 확실치 않아 후보를 차례로 봄
    tag = None
    for k in ("industryName", "upjongName", "sectorName", "categoryName", "itemName", "category", "researchCategory"):
        v = it.get(k)
        # 분류 이름 자체("투자전략", "시황정보" 등)는 태그로 안 씀 — 산업분석의 "자동차/철강금속"만 의미 있음
        if isinstance(v, str) and v.strip() and v.strip() not in (cat_label, it.get("researchCategory"), "데일리"):
            tag = v.strip()
            break
    return {
        "research_id": rid,
        "date": str(it.get("writeDate") or it.get("date") or "")[:10].replace(".", "-"),
        "title": title,
        "broker": it.get("brokerName") or it.get("brokerage") or "",
        "tag": tag,
        "summary": (it.get("previewContent") or "").strip() or None,
        "read_count": it.get("readCount"),
        "detail_url": it.get("endUrl") or (f"https://m.stock.naver.com/research/{path}/{rid}" if rid else None),
        "pdf_url": it.get("attachUrl"),
    }


def _research_pdf(path: str, rid):
    if not rid:
        return None
    try:
        res = requests.get(f"https://m.stock.naver.com/api/research/{path}/{rid}", headers=_naver_stock_headers(), timeout=6)
        if not res.ok:
            return None
        data = res.json()
        c = data.get("researchContent") if isinstance(data, dict) else None
        url = (c or {}).get("attachUrl") if isinstance(c, dict) else None
        if not url:
            m = re.search(r'(https?://[^"\s]+?\.pdf)', json.dumps(data, ensure_ascii=False), re.I)
            url = m.group(1) if m else None
        return url
    except Exception:
        return None


def fetch_research_list(category: str, page: int = 1, page_size: int = 40) -> dict:
    cat = RESEARCH_CATEGORIES.get(category)
    if not cat:
        raise ValueError(f"category는 {'/'.join(RESEARCH_CATEGORIES)} 중 하나여야 해요.")
    key = (category, page)
    cached = _research_cache.get(key)
    if cached and time.time() - cached[0] < _RESEARCH_CACHE_SEC:
        return cached[1]

    paths = [_research_path_ok[category]] if category in _research_path_ok else cat["paths"]
    rows, used, errors = [], None, []
    for path in paths:
        try:
            res = requests.get(
                f"https://m.stock.naver.com/api/research/{path}",
                headers=_naver_stock_headers(),
                params={"page": page, "pageSize": page_size},
                timeout=8,
            )
            if not res.ok:
                errors.append(f"{path}: HTTP {res.status_code}")
                continue
            items = _research_items_from(res.json())
            rows = [r for r in (_research_row(it, cat["label"], path) for it in items) if r]
            if rows:
                used = path
                _research_path_ok[category] = path
                break
            errors.append(f"{path}: 목록 비어있음")
        except Exception as e:
            errors.append(f"{path}: {e}")

    # PDF 주소가 목록에 없으면 앞쪽 20개만 상세에서 채움 (동시에 6개씩)
    need = [r for r in rows[:20] if not r.get("pdf_url") and r.get("research_id")]
    if need and used:
        with ThreadPoolExecutor(max_workers=6) as ex:
            for r, url in zip(need, ex.map(lambda x: _research_pdf(used, x["research_id"]), need)):
                if url:
                    r["pdf_url"] = url

    result = {"ok": True, "category": category, "label": cat["label"], "page": page, "items": rows}
    if not rows:
        result["note"] = "리포트 목록을 가져오지 못했어요."
        result["errors"] = errors
    else:
        _research_cache[key] = (time.time(), result)
    return result


@app.get("/api/research-list")
def research_list_endpoint(category: str = "strategy", page: int = 1, refresh: bool = False):
    """리서치 목록 — category: industry/strategy/economy/bond/market. 15분 캐시."""
    try:
        if refresh:
            _research_cache.pop((category, page), None)
        return fetch_research_list(category, max(1, page))
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/research-debug")
def research_debug():
    """디버그 전용 — 분류별 후보 주소들의 실제 응답 앞부분"""
    out = {}
    for cat, info in RESEARCH_CATEGORIES.items():
        for path in info["paths"]:
            url = f"https://m.stock.naver.com/api/research/{path}"
            try:
                r = requests.get(url, headers=_naver_stock_headers(), params={"page": 1, "pageSize": 3}, timeout=8)
                out[f"{cat}:{path}"] = {"status": r.status_code, "preview": r.text[:600]}
            except Exception as e:
                out[f"{cat}:{path}"] = {"error": str(e)}
    return out


# ══════════════════════════════════════════════════════════════════════════
# 🩺 데이터 상태 점검 + 텔레그램 알림
# ──────────────────────────────────────────────────────────────────────────
# 2026-07~10월에 시장 수급이 석 달간 멈춰 있었는데 아무도 몰랐던 일의 재발 방지.
# 평일 18:40 KST에 오늘 데이터가 다 들어왔는지 점검해서 텔레그램으로 보냄.
# Render 환경변수: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (없으면 점검만 하고 전송은 건너뜀)
# ══════════════════════════════════════════════════════════════════════════
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
HEALTH_REPORT_HOUR, HEALTH_REPORT_MINUTE = 18, 40
_health_sent = {"date": None}


def send_telegram(text: str, token: str = None, chat_id: str = None) -> bool:
    token = token or TELEGRAM_BOT_TOKEN
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not token or not chat_id:
        print("[telegram] 토큰/채팅ID 미설정 — 전송 건너뜀")
        return False
    # 텔레그램 한 메시지 최대 4096자 → 길면 줄 단위로 나눠서 여러 번 보냄
    if len(text) > 3800:
        chunks, cur = [], ""
        for line in text.split("\n"):
            if len(cur) + len(line) + 1 > 3800:
                chunks.append(cur)
                cur = ""
            cur += line + "\n"
        if cur.strip():
            chunks.append(cur)
        return all([_send_telegram_once(c, token, chat_id) for c in chunks])
    return _send_telegram_once(text, token, chat_id)


def _send_telegram_once(text: str, token: str, chat_id: str) -> bool:
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
        if not res.ok:
            print(f"[telegram] 전송 실패: {res.status_code} {res.text[:200]}")
        return res.ok
    except Exception as e:
        print(f"[telegram] 전송 오류: {e}")
        return False


def _latest_value(table: str, column: str, where: tuple = None):
    try:
        q = supabase.table(table).select(column)
        if where:
            q = q.eq(where[0], where[1])
        res = q.order(column, desc=True).limit(1).execute()
        return str(res.data[0][column]) if res.data else None
    except Exception as e:
        return f"조회실패({e})"


def build_health_report() -> dict:
    now = now_kst()
    today = now.strftime("%Y-%m-%d")
    # "있어야 할 최신 날짜" = KRX 기준 가장 최근 영업일. 휴장일(주말·공휴일)이나 장 마감 전엔
    # 전 영업일이 기준이라, 그날 데이터가 없다고 ❌로 잘못 알리지 않음 (2026-10-09 한글날에 오탐 확인)
    try:
        ltd = _latest_trading_day()
        expected = f"{ltd[:4]}-{ltd[4:6]}-{ltd[6:8]}"
    except Exception:
        expected = today
    trading_day = expected == today

    checks = []

    def add(name, latest, ok, hint=""):
        checks.append({"name": name, "latest": latest, "ok": ok, "hint": hint})

    mf = _latest_value("market_flow_trend", "trade_date", ("market", "ALL"))
    add("시장 수급", mf, bool(mf and mf[:10] >= expected), "KRX(pykrx) 로그인·응답 확인")
    kr = _latest_value("krx_daily_ranking", "scan_date")
    add("KRX 거래량 랭킹", kr, bool(kr and kr[:10] >= expected), "pykrx 응답 확인")
    nx = _latest_value("nxt_daily_ranking", "scan_date")
    add("NXT 스캔", nx, bool(nx and nx[:10] >= expected), "pykrx 응답 확인")
    fd = _latest_value("stock_fundamentals", "synced_at")
    add("재무제표", fd[:10] if fd else None, bool(fd and fd[:10] >= expected), "16시 이후 자동 동기화")
    try:
        cnt = (
            supabase.table("tick_minute_flow").select("stock_code", count="exact").eq("trade_date", today).limit(1).execute().count
        ) or 0
    except Exception:
        cnt = 0
    last_tick_min = (time.time() - _ws_status["last_tick"]) / 60 if _ws_status["last_tick"] else None
    add(
        "실시간 체결(정밀 세력평단)",
        f"오늘 {cnt:,}줄" + (f" · 마지막 체결 {last_tick_min:.0f}분 전" if last_tick_min is not None else ""),
        cnt > 0 or not trading_day,  # 휴장일엔 체결이 없는 게 정상
        "한투 웹소켓 연결 확인 (ALREADY IN USE 등)",
    )

    failed = [c for c in checks if not c["ok"]]
    return {"date": today, "expected_date": expected, "trading_day": trading_day, "ok": not failed, "checks": checks}


def format_health_message(rep: dict) -> str:
    head = "✅ JM 데이터 점검 — 모두 정상" if rep["ok"] else "⚠️ JM 데이터 점검 — 확인 필요"
    lines = [f"{head} ({rep['date']}" + ("" if rep.get("trading_day", True) else f" · 휴장일, {rep.get('expected_date')} 기준") + ")"]
    for c in rep["checks"]:
        mark = "✅" if c["ok"] else "❌"
        lines.append(f"{mark} {c['name']}: {c['latest'] or '없음'}" + ("" if c["ok"] else f"  → {c['hint']}"))
    return "\n".join(lines)


def _ensure_health_report_sent_today():
    now = now_kst()
    today = now.strftime("%Y-%m-%d")
    if now.weekday() >= 5 or _health_sent["date"] == today:
        return
    if (now.hour, now.minute) < (HEALTH_REPORT_HOUR, HEALTH_REPORT_MINUTE):
        return
    _health_sent["date"] = today  # 실패해도 하루 1번만 (알림 폭탄 방지)
    rep = build_health_report()
    if not rep["trading_day"]:
        print(f"[health] {today} 휴장일 — 점검 알림 생략")
        return
    send_telegram(format_health_message(rep))


@app.get("/api/health")
def health_endpoint(send: bool = False):
    """데이터 상태 점검 결과 — send=true면 텔레그램으로도 바로 보냄 (설정 확인용)"""
    rep = build_health_report()
    rep["telegram_configured"] = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    if send:
        rep["telegram_sent"] = send_telegram(format_health_message(rep))
    return rep


# ══════════════════════════════════════════════════════════════════════════
# 📊 DART 분기 실적 (매출·영업이익·순이익) — 흑자/적자·흑자전환/적자전환 정확 판별
# ──────────────────────────────────────────────────────────────────────────
# 금융감독원 OpenDART 공식 API. Render 환경변수 DART_API_KEY 필요.
# 종목코드 → DART 고유번호(corp_code) 변환표는 DART가 zip으로 주는 걸 하루 1번 받아 메모리에 둠.
# 분기 값: 1·2·3분기 보고서의 "3개월" 값을 쓰고, 4분기는 사업보고서(연간) − 1~3분기 합으로 계산.
# ══════════════════════════════════════════════════════════════════════════
import io
import zipfile
import xml.etree.ElementTree as ET

DART_API_KEY = os.environ.get("DART_API_KEY")
_dart_corp_map: dict = {}
_dart_corp_loaded_at = 0.0
_dart_quarter_cache: dict = {}  # code -> (시각, 결과)

REPORT_CODES = [("11013", 1), ("11012", 2), ("11014", 3), ("11011", 4)]  # 1분기/반기/3분기/사업보고서
ACCOUNT_ALIASES = {
    "revenue": ["매출액", "수익(매출액)", "영업수익", "매출", "순영업수익"],
    "op": ["영업이익", "영업이익(손실)", "영업손익"],
    "net": ["당기순이익", "당기순이익(손실)", "분기순이익", "반기순이익", "당기순손익", "분기순이익(손실)", "반기순이익(손실)"],
}


def _dart_corp_code(stock_code: str):
    global _dart_corp_loaded_at
    if not DART_API_KEY:
        raise RuntimeError("DART_API_KEY 환경변수가 설정되어 있지 않아요.")
    if not _dart_corp_map or time.time() - _dart_corp_loaded_at > 24 * 3600:
        res = requests.get("https://opendart.fss.or.kr/api/corpCode.xml", params={"crtfc_key": DART_API_KEY}, timeout=30)
        res.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
            xml_bytes = zf.read(zf.namelist()[0])
        root = ET.fromstring(xml_bytes)
        m = {}
        for el in root.iter("list"):
            sc = (el.findtext("stock_code") or "").strip()
            cc = (el.findtext("corp_code") or "").strip()
            if sc and cc:
                m[sc] = cc
        _dart_corp_map.clear()
        _dart_corp_map.update(m)
        _dart_corp_loaded_at = time.time()
        print(f"[dart] 고유번호표 {len(m)}개 로드")
    return _dart_corp_map.get(stock_code.upper())


def _to_eok(v):
    if v is None:
        return None
    s = str(v).replace(",", "").strip()
    if s in ("", "-"):
        return None
    try:
        return round(float(s) / 1e8, 1)
    except ValueError:
        return None


def _dart_report(corp_code: str, year: int, reprt_code: str):
    """한 보고서의 주요 계정 → {key: (3개월값, 누적값)} (연결재무제표 우선, 없으면 별도)"""
    res = requests.get(
        "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json",
        params={"crtfc_key": DART_API_KEY, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": reprt_code},
        timeout=10,
    )
    data = res.json()
    if data.get("status") != "000":
        return None
    rows = data.get("list") or []
    out = {}
    for fs in ("CFS", "OFS"):
        sub = [r for r in rows if r.get("fs_div") == fs and r.get("sj_div") in ("IS", "CIS")]
        if not sub:
            continue
        for key, names in ACCOUNT_ALIASES.items():
            if key in out:
                continue
            for nm in names:
                hit = next((r for r in sub if (r.get("account_nm") or "").replace(" ", "") == nm), None)
                if hit:
                    out[key] = (_to_eok(hit.get("thstrm_amount")), _to_eok(hit.get("thstrm_add_amount")))
                    break
        if out:
            out["_fs"] = fs
            break
    return out or None


def fetch_dart_quarterly(stock_code: str) -> dict:
    cached = _dart_quarter_cache.get(stock_code)
    if cached and time.time() - cached[0] < 12 * 3600:
        return cached[1]
    corp = _dart_corp_code(stock_code)
    if not corp:
        raise ValueError("DART에서 이 종목을 찾지 못했어요 (신규상장 직후이거나 상장사가 아닐 수 있어요).")

    this_year = now_kst().year
    quarters = []
    fs_used = None
    for year in (this_year - 2, this_year - 1, this_year):
        reps = {}
        for code_, q in REPORT_CODES:
            try:
                r = _dart_report(corp, year, code_)
            except Exception:
                r = None
            if r:
                reps[q] = r
                fs_used = fs_used or r.get("_fs")
            time.sleep(0.1)
        for key_q in (1, 2, 3, 4):
            if key_q not in reps:
                continue
            row = {"period": f"{year}Q{key_q}"}
            for key in ("revenue", "op", "net"):
                if key_q in (1, 2, 3):
                    three, cum = reps[key_q].get(key, (None, None))
                    val = three
                    # 3개월 값이 없고 누적만 있으면 앞 분기 누적과의 차이로 계산
                    if val is None and cum is not None and key_q > 1 and (key_q - 1) in reps:
                        prev_three, prev_cum = reps[key_q - 1].get(key, (None, None))
                        prev_total = prev_cum if prev_cum is not None else prev_three
                        val = round(cum - prev_total, 1) if prev_total is not None else None
                else:
                    annual = reps[4].get(key, (None, None))[0]
                    parts = [reps[q].get(key, (None, None))[0] if q in reps else None for q in (1, 2, 3)]
                    val = round(annual - sum(parts), 1) if annual is not None and all(v is not None for v in parts) else None
                row[key] = val
            if any(row.get(k) is not None for k in ("revenue", "op", "net")):
                quarters.append(row)

    quarters = quarters[-8:]
    status = None
    if quarters:
        last = quarters[-1]
        prev_year_period = f"{int(last['period'][:4]) - 1}{last['period'][4:]}"
        prev = next((q for q in quarters if q["period"] == prev_year_period), None)
        ln, pn = last.get("net"), (prev or {}).get("net")
        if ln is not None and pn is not None:
            if pn <= 0 < ln:
                status = "흑자전환"
            elif pn > 0 >= ln:
                status = "적자전환"
            elif ln > 0:
                status = "흑자지속"
            else:
                status = "적자지속"
        elif ln is not None:
            status = "흑자" if ln > 0 else "적자"
    ttm = quarters[-4:]
    ttm_net = round(sum(q["net"] for q in ttm), 1) if len(ttm) == 4 and all(q.get("net") is not None for q in ttm) else None
    result = {
        "ok": True,
        "code": stock_code,
        "fs": "연결" if fs_used == "CFS" else "별도" if fs_used == "OFS" else None,
        "unit": "억원",
        "quarters": quarters,
        "latest_status": status,
        "ttm_net": ttm_net,
    }
    _dart_quarter_cache[stock_code] = (time.time(), result)
    return result


@app.get("/api/dart-quarterly")
def dart_quarterly_endpoint(code: str):
    """최근 8분기 매출·영업이익·순이익(억원) + 흑자전환/적자전환 판별 (DART 공식 재무제표)"""
    try:
        return fetch_dart_quarterly((code or "").strip().upper())
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 🗓 주요 일정 캘린더 (오늘 + 다가오는 일정)
# ──────────────────────────────────────────────────────────────────────────
#   ① 미국 경제지표 발표일 — FRED 공식 발표 일정(releases/dates, FRED_API_KEY 사용).
#      발표 시각은 지표별 관례(미 동부시간)를 한국시간으로 바꿔서 붙임 (서머타임 자동 반영).
#   ② 미국 대형주 실적발표 — 나스닥 실적 캘린더 (시가총액 1,000억달러 이상만)
#   ③ 국내 공모주 청약 — 38커뮤니케이션 공모주 청약일정
# 출처마다 실패해도 나머지는 보여줌. 문제 생기면 /api/calendar-debug 로 확인.
# ══════════════════════════════════════════════════════════════════════════
try:
    from zoneinfo import ZoneInfo

    _ET = ZoneInfo("America/New_York")
    _KST_ZONE = ZoneInfo("Asia/Seoul")
except Exception:  # 서버에 시간대 정보가 없을 때 대비 — 아래 _us_to_kst에서 서머타임을 직접 계산
    _ET = None
    _KST_ZONE = KST
_calendar_cache = {"at": 0.0, "key": None, "data": None}

# (FRED 발표 이름에 들어있는 글자, 한글 이름, 미 동부 발표시각, 중요(⚡), 영향 설명)
US_RELEASES = [
    ("Consumer Price Index", "소비자물가지수(CPI)", "08:30", True, "인플레이션·금리 정책에 큰 영향"),
    ("Employment Situation", "고용보고서(비농업고용·실업률)", "08:30", True, "고용 시장·금리 정책에 큰 영향"),
    ("Producer Price Index", "생산자물가지수(PPI)", "08:30", True, "인플레이션 선행 지표"),
    ("Advance Monthly Sales for Retail", "소매판매", "08:30", True, "소비 지출·경기 판단"),
    ("Gross Domestic Product", "GDP", "08:30", True, "경제 성장률"),
    ("Personal Income and Outlays", "개인소득·지출(PCE 물가)", "08:30", True, "연준이 보는 핵심 물가"),
    ("Unemployment Insurance Weekly Claims", "주간 신규 실업수당 청구건수", "08:30", False, "고용 시장 흐름"),
    ("New Residential Construction", "주택착공·허가", "08:30", False, "주택 경기"),
    ("Job Openings and Labor Turnover", "JOLTS 구인건수", "10:00", False, "노동 수요"),
    ("Industrial Production", "산업생산", "09:15", False, "제조업 경기"),
    ("Existing Home Sales", "기존주택판매", "10:00", False, "부동산 경기·금리 영향"),
    ("Surveys of Consumers", "미시간대 소비자심리", "10:00", False, "소비 심리"),
]

# FOMC 금리 결정일 (회의 둘째 날, 미 동부 14:00 발표) — FRED의 "FOMC Press Release"는 매일 날짜가 잡혀서
# 못 씀(2026-10-11 매일 FOMC로 뜨는 오류 확인). 연준 공식 일정표 기준으로 직접 적어둠 → ⚠️ 매년 갱신 필요
FOMC_DECISION_DATES = [
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
]


def _us_to_kst(date_str: str, hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    base = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=h, minute=m)
    if _ET is not None:
        return base.replace(tzinfo=_ET).astimezone(_KST_ZONE)
    # 미국 서머타임: 3월 둘째 일요일 ~ 11월 첫째 일요일 (UTC-4), 그 외 UTC-5
    y = base.year
    mar = datetime(y, 3, 8) + timedelta(days=(6 - datetime(y, 3, 8).weekday()) % 7)
    nov = datetime(y, 11, 1) + timedelta(days=(6 - datetime(y, 11, 1).weekday()) % 7)
    offset = -4 if mar <= base < nov else -5
    return base.replace(tzinfo=timezone(timedelta(hours=offset))).astimezone(KST)


_us_macro_cache = {"at": 0.0, "rows": None}  # FRED 발표일 원본 (6시간 보관, 실패하면 마지막 성공값 사용)


def _fred_release_dates(start: str, end: str) -> list:
    """
    FRED 발표 일정 원본. ⚠️ FRED 서버가 가끔 10초 넘게 걸려서 타임아웃이 났음(2026-10-11) →
    25초까지 기다리고 2번 더 시도, 그래도 실패하면 마지막으로 성공했던 값을 씀.
    발표 일정은 자주 안 바뀌니 성공하면 6시간 보관.
    """
    c = _us_macro_cache
    if c["rows"] is not None and time.time() - c["at"] < 6 * 3600 and c.get("start") <= start and c.get("end") >= end:
        return c["rows"]
    wide_end = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=14)).strftime("%Y-%m-%d")  # 넉넉히 받아둠
    last_err = None
    for attempt in range(3):
        try:
            res = requests.get(
                "https://api.stlouisfed.org/fred/releases/dates",
                params={
                    "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "realtime_start": start,
                    "realtime_end": wide_end,
                    "include_release_dates_with_no_data": "true",
                    "sort_order": "asc",
                    "limit": 1000,
                },
                timeout=25,
            )
            res.raise_for_status()
            rows = res.json().get("release_dates", [])
            c.update({"at": time.time(), "rows": rows, "start": start, "end": wide_end})
            return rows
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    if c["rows"] is not None:
        print(f"[calendar] FRED 실패, 마지막 성공값 사용: {last_err}")
        return c["rows"]
    raise last_err


def _fetch_us_economic(start: str, end: str) -> list:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY 미설정")
    out, seen = [], set()
    rows = [r for r in _fred_release_dates(start, end) if start <= (r.get("date") or "") <= end]
    # 매일 날짜가 잡히는 발표(데이터 없는 날 포함)는 실제 발표일이 아니라서 걸러냄:
    # 같은 지표가 기간 안에 3번 넘게 나오면 버리고, 주간 실업수당은 목요일만 남김
    from collections import Counter
    cnt = Counter()
    for r in rows:
        for key, *_ in US_RELEASES:
            if key.lower() in (r.get("release_name") or "").lower():
                cnt[key] += 1
                break
    filtered = []
    for r in rows:
        name = (r.get("release_name") or "").lower()
        key = next((k for k, *_ in US_RELEASES if k.lower() in name), None)
        if not key:
            continue
        if key == "Unemployment Insurance Weekly Claims":
            if datetime.strptime(r["date"], "%Y-%m-%d").weekday() != 3:
                continue
        elif cnt[key] > 2:
            continue
        filtered.append(r)
    for d in FOMC_DECISION_DATES:
        if start <= d <= end:
            kst = _us_to_kst(d, "14:00")
            out.append({"date": kst.strftime("%Y-%m-%d"), "time": kst.strftime("%H:%M"), "title": "美) FOMC 금리 결정",
                        "major": True, "note": "기준금리 결정 — 시장 전체에 큰 영향", "kind": "us_macro"})
    for r in filtered:
        name = r.get("release_name") or ""
        for key, kr, et, major, why in US_RELEASES:
            if key.lower() in name.lower():
                k = (kr, r["date"])
                if k in seen:
                    break
                seen.add(k)
                kst = _us_to_kst(r["date"], et)
                out.append(
                    {
                        "date": kst.strftime("%Y-%m-%d"),
                        "time": kst.strftime("%H:%M"),
                        "title": f"美) {kr}",
                        "major": major,
                        "note": why,
                        "kind": "us_macro",
                    }
                )
                break
    return out


def _fetch_us_earnings(start: str, end: str, min_cap: float = 100e9) -> list:
    out = []
    d = datetime.strptime(start, "%Y-%m-%d")
    last = datetime.strptime(end, "%Y-%m-%d")
    while d <= last:
        if d.weekday() < 5:
            try:
                res = requests.get(
                    "https://api.nasdaq.com/api/calendar/earnings",
                    params={"date": d.strftime("%Y-%m-%d")},
                    headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                    timeout=8,
                )
                rows = ((res.json() or {}).get("data") or {}).get("rows") or []
            except Exception as e:
                print(f"[calendar] 나스닥 실적 {d.date()} 실패: {e}")
                rows = []
            for r in rows:
                cap = str(r.get("marketCap") or "").replace("$", "").replace(",", "")
                try:
                    cap_v = float(cap)
                except ValueError:
                    continue
                if cap_v < min_cap:
                    continue
                when = (r.get("time") or "").lower()
                tlabel = "장전" if "pre" in when else "장후" if "after" in when else ""
                out.append(
                    {
                        "date": d.strftime("%Y-%m-%d"),
                        "time": None,
                        "title": f"美) {r.get('name') or r.get('symbol')} 실적발표" + (f" ({tlabel})" if tlabel else ""),
                        "major": cap_v >= 500e9,
                        "note": f"{r.get('symbol')} · 시가총액 ${cap_v / 1e9:,.0f}B",
                        "kind": "us_earnings",
                    }
                )
        d += timedelta(days=1)
    return out


def _fetch_kr_ipo(start: str, end: str) -> list:
    if BeautifulSoup is None:
        raise RuntimeError("beautifulsoup4 미설치")
    res = requests.get(
        "http://www.38.co.kr/html/fund/index.htm",
        params={"o": "k"},
        headers={"User-Agent": "Mozilla/5.0", "Referer": "http://www.38.co.kr/"},
        timeout=10,
    )
    html = res.content.decode("euc-kr", errors="replace")
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        texts = [td.get_text(" ", strip=True) for td in tds]
        m = None
        for t in texts:
            m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})\s*~\s*(?:(\d{2})\.)?(\d{2})", t)
            if m:
                break
        if not m:
            continue
        name = texts[0].replace("(유가)", "").replace("(코스닥)", "").strip()
        if not name or len(name) > 30:
            continue
        y, mo, d1 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        mo2 = int(m.group(4)) if m.group(4) else mo
        d2 = int(m.group(5))
        try:
            s1 = datetime(y, mo, d1)
            s2 = datetime(y if mo2 >= mo else y + 1, mo2, d2)
        except ValueError:
            continue
        day = s1
        while day <= s2:
            ds = day.strftime("%Y-%m-%d")
            if start <= ds <= end and day.weekday() < 5:
                out.append({"date": ds, "time": None, "title": f"공모청약 : {name}", "major": False, "note": None, "kind": "kr_ipo"})
            day += timedelta(days=1)
    return out


# 🇰🇷 국내 일정 — 한국은행 금통위(연 8회, ⚠️ 매년 갱신), 옵션만기일·동시만기일(매월 둘째 목요일),
# 수출입 동향(매월 1일 산업부, 11일·21일 관세청 잠정)
BOK_MPC_DATES = [
    "2026-01-15", "2026-02-26", "2026-04-10", "2026-05-28",
    "2026-07-16", "2026-08-27", "2026-10-22", "2026-11-26",
]


def _second_thursday(y: int, m: int) -> datetime:
    d = datetime(y, m, 1)
    first_thu = d + timedelta(days=(3 - d.weekday()) % 7)
    return first_thu + timedelta(days=7)


def _fetch_kr_schedule(start: str, end: str) -> list:
    out = []
    for d in BOK_MPC_DATES:
        if start <= d <= end:
            out.append({"date": d, "time": "10:00", "title": "韓) 한국은행 금통위 기준금리 결정", "major": True,
                        "note": "기준금리·환율·은행주에 영향 (총재 기자간담회 오전 11시경)", "kind": "kr_macro"})
    s_dt = datetime.strptime(start, "%Y-%m-%d")
    e_dt = datetime.strptime(end, "%Y-%m-%d")
    y, m = s_dt.year, s_dt.month
    while datetime(y, m, 1) <= e_dt:
        thu = _second_thursday(y, m).strftime("%Y-%m-%d")
        if start <= thu <= end:
            quad = m in (3, 6, 9, 12)
            out.append({"date": thu, "time": None,
                        "title": "韓) 선물·옵션 동시만기일" if quad else "韓) 옵션만기일",
                        "major": quad, "note": "장 마감 무렵 프로그램 매물·변동성 주의", "kind": "kr_macro"})
        for day, title, t in ((1, "韓) 월간 수출입 동향 (산업부)", "09:00"),
                              (11, "韓) 1~10일 수출 잠정치 (관세청)", "09:00"),
                              (21, "韓) 1~20일 수출 잠정치 (관세청)", "09:00")):
            ds = f"{y}-{m:02d}-{day:02d}"
            if start <= ds <= end:
                out.append({"date": ds, "time": t, "title": title, "major": day == 1,
                            "note": "반도체 등 수출 흐름 확인", "kind": "kr_macro"})
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def fetch_calendar(days: int = 7) -> dict:
    now = now_kst()
    start = now.strftime("%Y-%m-%d")
    end = (now + timedelta(days=days)).strftime("%Y-%m-%d")
    key = (start, days)
    if _calendar_cache["key"] == key and time.time() - _calendar_cache["at"] < 30 * 60:
        return _calendar_cache["data"]
    events, errors = [], []
    for label, fn in (
        ("국내 일정", _fetch_kr_schedule),
        ("미국 경제지표", _fetch_us_economic),
        ("미국 실적", _fetch_us_earnings),
        ("공모주", _fetch_kr_ipo),
    ):
        try:
            events += fn(start, end)
        except Exception as e:
            errors.append(f"{label}: {e}")
            print(f"[calendar] {label} 실패: {e}")
    events.sort(key=lambda e: (e["date"], e["time"] or "99:99", not e["major"]))
    data = {"ok": True, "start": start, "end": end, "events": events, "errors": errors}
    # 실패한 출처가 있으면 5분만 기억 → 다음 요청 때 금방 다시 시도
    _calendar_cache.update({"at": time.time() - (25 * 60 if errors else 0), "key": key, "data": data})
    return data


@app.get("/api/calendar")
def calendar_endpoint(days: int = 7):
    """주요 일정 (오늘부터 days일) — 미국 경제지표·미국 대형주 실적·국내 공모청약"""
    try:
        return fetch_calendar(max(1, min(days, 21)))
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/calendar-debug")
def calendar_debug():
    now = now_kst()
    start = now.strftime("%Y-%m-%d")
    end = (now + timedelta(days=7)).strftime("%Y-%m-%d")
    out = {}
    for label, fn in (("us_macro", _fetch_us_economic), ("us_earnings", _fetch_us_earnings), ("kr_ipo", _fetch_kr_ipo)):
        try:
            rows = fn(start, end)
            out[label] = {"count": len(rows), "sample": rows[:5]}
        except Exception as e:
            out[label] = {"error": str(e)}
    return out


# ══════════════════════════════════════════════════════════════════════════
# 📨 텔레그램 정기 브리핑
# ──────────────────────────────────────────────────────────────────────────
#   06:15  🌙 미국 증시 모닝 브리핑 (화~토 — 전날 미국장 마감 숫자)
#   09:30  🚀 아침 상승 랭킹 (시가 대비 상승률 상위 20)
#   10:00  🌅 오전장 테마 이슈
#   08:00  📋 장전 리포트 (어제 수급 한 줄 + 오늘 주요 일정 + 증권가 목표가 상향·하향·신규)
#   14:00  🗓 주요 일정 (오늘 + 다가오는 7일)
#   16:10  💰 오늘의 수급 요약 (코스피·코스닥 개인/외국인/기관 + 연속일수 + 기관 주도 주체 + 사모 TOP5)
#   20:00  🔥 오늘의 테마 이슈 (등락률 상위 테마 + 테마별 주도주)
# 보낸 기록은 Supabase telegram_sent_log에 남겨서, 서버가 재시작돼도 같은 알림을 두 번 안 보냄.
# 수동 미리보기: /api/brief?kind=morning|targets|flow|theme  (&send=true 면 텔레그램으로도 보냄)
# ══════════════════════════════════════════════════════════════════════════
BRIEF_SCHEDULE = {
    "morning": (6, 15),
    "targets": (8, 0),  # 📋 장전 리포트 (목표가 + 오늘 일정 + 어제 수급)
    "rally": (9, 30),  # 🚀 아침 상승 랭킹 (09:00~09:30)
    "theme_am": (10, 0),  # 🌅 오전장 테마 이슈 (20:00 테마 이슈의 오전판 — 변동성 미리 보기)
    "schedule": (14, 0),
    "flow": (16, 10),
    "theme": (20, 0),
}


def _brief_already_sent(kind: str, ref_date: str) -> bool:
    try:
        res = supabase.table("telegram_sent_log").select("kind").eq("kind", kind).eq("ref_date", ref_date).limit(1).execute()
        return bool(res.data)
    except Exception:
        return False


def _mark_brief_sent(kind: str, ref_date: str):
    try:
        supabase.table("telegram_sent_log").upsert({"kind": kind, "ref_date": ref_date}).execute()
    except Exception as e:
        print(f"[brief] 발송 기록 실패: {e}")


def _fmt_eok(v: float, signed: bool = True) -> str:
    """억원 → '+3조6,411억' 형태"""
    sign = ("+" if v > 0 else "-" if v < 0 else "") if signed else ""
    a = abs(v)
    if a >= 10000:
        jo = int(a // 10000)
        rest = round(a - jo * 10000)
        return f"{sign}{jo}조{rest:,}억" if rest else f"{sign}{jo}조"
    return f"{sign}{a:,.0f}억"


WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def _kr_date(ymd: str) -> str:
    d = datetime.strptime(ymd[:10], "%Y-%m-%d")
    return f"{d.year}.{d.month:02d}.{d.day:02d}.{WEEKDAY_KR[d.weekday()]}"


# ── 🌙 미국 증시 모닝 브리핑 ──
MORNING_SYMBOLS = [
    ("다우", "^DJI", "p"),
    ("S&P500", "^GSPC", "p"),
    ("나스닥", "^IXIC", "p"),
    ("러셀2000", "^RUT", "p"),
    ("필라델피아 반도체", "^SOX", "p"),
    ("VIX", "^VIX", ""),
    ("미 10년물 금리", "^TNX", "%"),
    ("달러인덱스", "DX-Y.NYB", ""),
    ("WTI 유가", "CL=F", "$"),
    ("금", "GC=F", "$"),
    ("원/달러", "KRW=X", "원"),
    ("비트코인", "BTC-USD", "$"),
]
# 🇪🇺 유럽 — 프랑스 재정 이슈 등 유럽발 금리·위험 흐름 확인용 (애널리스트 시황에 자주 나오는 지수)
EUROPE_SYMBOLS = [
    ("유로스톡스50", "^STOXX50E", "p"),
    ("독일 DAX", "^GDAXI", "p"),
    ("프랑스 CAC40", "^FCHI", "p"),
    ("영국 FTSE100", "^FTSE", "p"),
    ("유로/달러", "EURUSD=X", ""),
]


def _yahoo_quote(symbol: str):
    res = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(symbol)}",
        params={"range": "5d", "interval": "1d"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=8,
    )
    res.raise_for_status()
    r = res.json()["chart"]["result"][0]
    closes = [c for c in (r["indicators"]["quote"][0].get("close") or []) if c is not None]
    stamps = r.get("timestamp") or []
    if len(closes) < 2:
        return None
    last, prev = closes[-1], closes[-2]
    last_day = datetime.fromtimestamp(stamps[len(closes) - 1], tz=timezone(timedelta(hours=-5))).strftime("%Y-%m-%d") if stamps else ""
    return {"price": last, "pct": (last - prev) / prev * 100 if prev else 0.0, "date": last_day}


def build_morning_brief():
    rows, us_date = [], None
    for label, sym, unit in MORNING_SYMBOLS:
        try:
            q = _yahoo_quote(sym)
        except Exception as e:
            print(f"[brief:morning] {sym} 실패: {e}")
            q = None
        if not q:
            continue
        if sym == "^GSPC":
            us_date = q["date"]
        arrow = "🔺" if q["pct"] > 0 else "🔻" if q["pct"] < 0 else "▫️"
        price = q["price"]
        if unit == "%":
            val = f"{price:.3f}%"
        elif unit == "원":
            val = f"{price:,.2f}원"
        elif unit == "$":
            val = f"${price:,.2f}"
        else:
            val = f"{price:,.2f}" + ("p" if unit == "p" else "")
        rows.append(f"{arrow} {label} {val} ({q['pct']:+.2f}%)")
    if not rows:
        return None, None
    idx = [r for r in rows[:5]]
    up = sum(1 for r in idx[:3] if r.startswith("🔺"))
    mood = "3대 지수 모두 상승" if up == 3 else "3대 지수 모두 하락" if up == 0 else "3대 지수 혼조"
    eu_rows = []
    for label, sym, unit in EUROPE_SYMBOLS:
        try:
            q = _yahoo_quote(sym)
        except Exception:
            q = None
        if not q:
            continue
        arrow = "🔺" if q["pct"] > 0 else "🔻" if q["pct"] < 0 else "▫️"
        val = f"{q['price']:,.2f}" + ("p" if unit == "p" else "") if label != "유로/달러" else f"{q['price']:.4f}"
        eu_rows.append(f"{arrow} {label} {val} ({q['pct']:+.2f}%)")
    text = (
        f"☀️ 미국 증시 모닝 브리핑 ({us_date or '-'} 미국장 마감)\n━━━━━━━━━━━━\n"
        f"📌 {mood}\n\n" + "\n".join(rows[:5])
        + ("\n\n🇪🇺 유럽 마감\n" + "\n".join(eu_rows) if eu_rows else "")
        + "\n\n" + "\n".join(rows[5:])
    )
    return text, us_date


# ── 🎯 증권가 목표가 ──
def build_targets_brief():
    items = []
    for page in (1, 2, 3):
        try:
            res = requests.get(
                "https://m.stock.naver.com/api/research/company",
                headers=_naver_stock_headers(),
                params={"page": page, "pageSize": 50},
                timeout=8,
            )
            batch = res.json() if res.ok else []
        except Exception:
            batch = []
        if not isinstance(batch, list) or not batch:
            break
        items += batch
    if not items:
        return None, None
    # 아침 8시엔 오늘 리포트가 몇 개 안 올라와 있어서(2026-10-12 1건만 잡힘), 직전 영업일 리포트 전체 + 오늘 것까지 봄
    today_s = now_kst().strftime("%Y-%m-%d")
    dates = sorted({str(it.get("writeDate", ""))[:10] for it in items}, reverse=True)
    prev_dates = [d for d in dates if d < today_s]
    latest = prev_dates[0] if prev_dates else dates[0]
    day_items = [it for it in items if str(it.get("writeDate", ""))[:10] >= latest][:70]

    def detail(it):
        d = _naver_research_detail_json(it.get("researchId"))
        d["prev_target_price"] = None
        try:  # 같은 증권사가 이 종목에 낸 직전 리포트의 목표가
            hist = _naver_research_json(str(it.get("itemCode") or ""))
            older = [h for h in hist if h.get("broker") == it.get("brokerName") and str(h.get("research_id")) != str(it.get("researchId")) and (h.get("date") or "") <= str(it.get("writeDate"))[:10]]
            if older:
                d["prev_target_price"] = _naver_research_detail_json(older[0]["research_id"]).get("target_price")
        except Exception:
            pass
        return {**d, "name": it.get("itemName"), "code": it.get("itemCode"), "broker": it.get("brokerName"), "title": it.get("title")}

    with ThreadPoolExecutor(max_workers=6) as ex:
        rows = list(ex.map(detail, day_items))
    ups, downs, news = [], [], []
    for r in rows:
        tp, prev = r.get("target_price"), r.get("prev_target_price")
        if not tp:
            continue
        if prev and tp > prev:
            ups.append(((tp / prev - 1) * 100, r))
        elif prev and tp < prev:
            downs.append(((tp / prev - 1) * 100, r))
        else:
            news.append(r)  # 직전 목표가가 없거나(신규) 같은(유지) 경우
    ups.sort(key=lambda x: -x[0])

    def line(r, pct=None):
        tail = f" ({pct:+.0f}%)" if pct is not None else ""
        prev = f"{r['prev_target_price']:,} → " if r.get("prev_target_price") else ""
        price_now = r.get("price_at_write")
        upside = f" · 상승여력 {((r['target_price'] / price_now) - 1) * 100:+.0f}%" if price_now else ""
        link = f"\n   🔗 {FRONTEND_URL}/stock-analysis?code={r['code']}" if r.get("code") else ""
        return f"· {r['name']} · {r['broker']}\n   {prev}{r['target_price']:,}원{tail} {r.get('opinion') or ''}{upside}{link}"

    parts = [f"🎯 증권가 목표가 ({_kr_date(latest)}~ 리포트 {len(day_items)}건)\n━━━━━━━━━━━━"]
    if ups:
        parts.append("🔺 목표가 상향 TOP\n" + "\n".join(line(r, pct) for pct, r in ups[:10]))
    if downs:
        downs.sort(key=lambda x: x[0])
        parts.append("🔻 목표가 하향\n" + "\n".join(line(r, pct) for pct, r in downs[:5]))
    if news:
        parts.append("🆕 신규·유지\n" + "\n".join(line(r) for r in news[:5]))
    if len(parts) == 1:
        parts.append("목표가가 있는 리포트가 없어요.")
    return "\n\n".join(parts), latest


# ── 💰 오늘의 수급 요약 ──
INST_SUB = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금"]


def _streak(market: str, field: str, today_val: float) -> int:
    try:
        rows = (
            supabase.table("market_flow_trend").select(f"trade_date,{field}").eq("market", market)
            .order("trade_date", desc=True).limit(30).execute().data
        )
    except Exception:
        return 0
    sign = 1 if today_val > 0 else -1
    n = 0
    for r in rows:
        v = float(r.get(field) or 0)
        if v * sign > 0:
            n += 1
        else:
            break
    return n


def build_flow_brief():
    ltd = _latest_trading_day()
    day = f"{ltd[:4]}-{ltd[4:6]}-{ltd[6:8]}"
    sync_market_flow(days=5)  # 오늘 값 확정치로 한 번 더 갱신
    lines = [f"💰 오늘의 수급 ({_kr_date(day)})\n━━━━━━━━━━━━"]
    summary = []
    for market, label in (("KOSPI", "코스피"), ("KOSDAQ", "코스닥")):
        try:
            row = (
                supabase.table("market_flow_trend").select("*").eq("market", market).eq("trade_date", day)
                .limit(1).execute().data
            )
        except Exception:
            row = []
        if not row:
            lines.append(f"▪ {label}: 오늘 데이터 아직 없음")
            continue
        r = row[0]
        lines.append(f"▪ {label}")
        for nm, field in (("개인", "individual"), ("외국인", "foreign_amt"), ("기관", "institution")):
            v = float(r.get(field) or 0)
            st = _streak(market, field, v)
            st_txt = f" ⟨{st}거래일 연속 순{'매수' if v > 0 else '매도'}⟩" if st >= 2 else ""
            lines.append(f"  {nm} {_fmt_eok(v)}{st_txt}")
            if nm == "외국인":
                summary.append(f"{label} 순{'매수' if v > 0 else '매도'}")
        # 기관 안에서 누가 주도했는지
        try:
            df = _market_flow_df(ltd, ltd, market)
            if df is not None and not df.empty:
                last = df.iloc[-1]
                inst_total = sum(_pick(last, c) for c in INST_SUB)
                if inst_total:
                    same = [(c, _pick(last, c)) for c in INST_SUB if _pick(last, c) * inst_total > 0]
                    if same:
                        lead, lv = max(same, key=lambda x: abs(x[1]))
                        share = min(100, abs(lv) / abs(inst_total) * 100)
                        last_ch = lead[-1]
                        josa = "이" if "가" <= last_ch <= "힣" and (ord(last_ch) - 0xAC00) % 28 else "가"
                        lines.append(f"  └ 기관 {'매수' if inst_total > 0 else '매도'}는 {lead}{josa} 주도 ({share:.0f}%)")
        except Exception as e:
            print(f"[brief:flow] 기관 세부 실패: {e}")
    if summary:
        lines.insert(1, "외국인 · " + " / ".join(summary) + "\n")
    # 사모 TOP5 (최근 2거래일)
    try:
        dates = (
            supabase.table("market_flow_trend").select("trade_date").eq("market", "ALL")
            .order("trade_date", desc=True).limit(2).execute().data
        )
        from_ymd = str(dates[-1]["trade_date"]).replace("-", "") if dates else ltd
        df = _krx_df(stock.get_market_net_purchases_of_equities, from_ymd, ltd, "ALL", "사모")
        if df is not None and not df.empty:
            col = "순매수거래대금" if "순매수거래대금" in df.columns else df.columns[-1]
            top = df.sort_values(col, ascending=False).head(5)
            lines.append("\n👽 사모 순매수 TOP5 (2거래일)")
            for i, (code, rr) in enumerate(top.iterrows(), start=1):
                nm = rr.get("종목명") if isinstance(rr.get("종목명"), str) else _safe_ticker_name(code)
                lines.append(f"{i} {nm} {_fmt_eok(float(rr[col]) / 1e8, signed=False)}")
    except Exception as e:
        print(f"[brief:flow] 사모 TOP5 실패: {e}")
    return "\n".join(lines), day


# ── 🔥 오늘의 테마 이슈 ──
def _to_eok_from_naver(v: float, price: float = 0, volume: float = 0) -> float:
    """네이버 거래대금 단위가 종목마다 원/백만원으로 섞여 와서, 주가×거래량과 더 가까운 쪽으로 판단"""
    import math
    est = price * volume
    if v <= 0:
        return 0.0
    if est > 0:
        won = v if abs(math.log(v / est)) <= abs(math.log(v * 1e6 / est)) else v * 1e6
    else:
        won = v * 1e6 if v < 1e7 else v
    return won / 1e8


def build_theme_brief(top_n: int = 5, title: str = "🔥 오늘의 테마 이슈"):
    themes = fetch_naver_theme_list(limit=25)
    themes = sorted(themes, key=lambda t: t["change_pct"], reverse=True)[:top_n]
    now = now_kst()
    parts = [f"{title} {now.month}/{now.day}({WEEKDAY_KR[now.weekday()]} {now.strftime('%H:%M')})\n━━━━━━━━━━━━"]
    for t in themes:
        try:
            stocks, _ = fetch_naver_theme_detail(t["theme_no"], limit=30)
        except Exception:
            stocks = []
        stocks = sorted(stocks, key=lambda x: x["change_pct"], reverse=True)[:4]
        for x in stocks:
            x["eok"] = _to_eok_from_naver(x["trading_value"], x.get("price", 0), x.get("volume", 0))
        total = sum(x["eok"] for x in stocks)
        top_val = max((x["eok"] for x in stocks), default=0)
        lines = [f"📌 {t['theme_name']} {t['change_pct']:+.2f}% · 주도주 거래대금 {_fmt_eok(total, signed=False)}"]
        for x in stocks:
            tags = (" 👑" if x["eok"] == top_val and top_val else "") + (" 🔥" if x["change_pct"] >= 29 else "")
            lines.append(f"{x['stock_name']} {x['change_pct']:+.2f}% · {_fmt_eok(x['eok'], signed=False)}{tags}")
        parts.append("\n".join(lines))
    return "\n\n".join(parts), now.strftime("%Y-%m-%d")


def _event_line(e: dict) -> str:
    star = " ⚡" if e.get("major") else ""
    t = f" ({e['time']})" if e.get("time") else ""
    line = f"· {e['title']}{star}{t}"
    if e.get("note") and e.get("kind") in ("us_macro", "kr_macro"):
        line += f"\n  ↳ {e['note']}"
    return line


def _date_head(ymd: str) -> str:
    d = datetime.strptime(ymd, "%Y-%m-%d")
    return f"{d.month}/{d.day}({WEEKDAY_KR[d.weekday()]})"


def build_schedule_brief(max_per_day: int = 6):
    cal = fetch_calendar(7)
    today = cal["start"]
    by_day = {}
    for e in cal["events"]:
        by_day.setdefault(e["date"], []).append(e)
    parts = [f"🗓 주요 일정 ({_kr_date(today)})\n━━━━━━━━━━━━"]
    todays = by_day.get(today, [])
    parts.append("📅 오늘\n" + ("\n".join(_event_line(e) for e in todays) if todays else "· 큰 일정 없음"))
    up = []
    for day in sorted(d for d in by_day if d > today):
        evs = sorted(by_day[day], key=lambda e: (not e["major"], e["time"] or "99:99"))
        lines = [_date_head(day)] + [_event_line(e) for e in evs[:max_per_day]]
        if len(evs) > max_per_day:
            lines.append(f"외 {len(evs) - max_per_day}건")
        up.append("\n".join(lines))
    if up:
        parts.append("🗓 다가오는 일정\n\n" + "\n\n".join(up))
    # 📢 최근 7일 실적발표 예고·IR 공시 (날짜는 공시 원문에 있어서 링크로 확인)
    try:
        _, rows = build_earnings_digest(days=7)
        plans = [r for r in rows if _classify_disclosure(r.get("report_nm")) == "plan"][:10]
        if plans:
            parts.append("📢 실적발표 예고·IR 공시 (최근 7일)\n" + "\n".join(_disclosure_line(r, set()) for r in plans))
    except Exception as e:
        print(f"[brief:schedule] 실적 예고 실패: {e}")
    if cal.get("errors"):
        parts.append("※ 일부 일정 출처를 못 불러왔어요: " + " / ".join(e[:120] for e in cal["errors"]))
    return "\n\n".join(parts), today


def _stock_link(code: str) -> str:
    return f"{FRONTEND_URL}/stock-analysis?code={code}"


def _build_watch_candidates() -> str:
    """
    장전 리포트용 "오늘 볼 만한 후보" — 예측이 아니라, 밤사이·전일 데이터에서 눈에 띄는 것만 모음:
      ① 밤사이 미국 섹터 힌트 (필라델피아 반도체·나스닥·유가 → 국내 관련 업종)
      ② 전일 강세 테마 TOP3 + 주도주
      ③ 전일 장 마감 후~오늘 아침 실적 공시 종목 (숫자 포함)
      ④ 전일 외국인 순매수 TOP5
    """
    lines = ["📌 오늘 볼 만한 후보 (데이터로 고른 것 · 추천 아님)"]
    # ① 미국 섹터 힌트
    hints = []
    for sym, label, kr in (("^SOX", "필라델피아 반도체", "반도체"), ("^IXIC", "나스닥", "성장·기술주"), ("CL=F", "WTI 유가", "정유·에너지")):
        try:
            q = _yahoo_quote(sym)
        except Exception:
            q = None
        if q and abs(q["pct"]) >= 1.0:
            hints.append(f"{label} {q['pct']:+.1f}% → {kr} {'강세' if q['pct'] > 0 else '약세'} 가능성")
    if hints:
        lines.append("🌙 밤사이 미국: " + " / ".join(hints))
    # ② 전일 강세 테마
    try:
        themes = sorted(fetch_naver_theme_list(limit=25), key=lambda t: t["change_pct"], reverse=True)[:3]
        tl = []
        for t in themes:
            try:
                stocks, _ = fetch_naver_theme_detail(t["theme_no"], limit=20)
            except Exception:
                stocks = []
            top = sorted(stocks, key=lambda x: x["change_pct"], reverse=True)[:2]
            tl.append(f"· {t['theme_name']} {t['change_pct']:+.1f}%")
            for x in top:
                tl.append(f"   {x['stock_name']} {x['change_pct']:+.1f}% 🔗 {_stock_link(x['stock_code'])}")
        if tl:
            lines.append("🔥 전일 강세 테마\n" + "\n".join(tl))
    except Exception as e:
        print(f"[brief:candidates] 테마 실패: {e}")
    # ③ 전일 장 마감 후 ~ 오늘 아침 실적 공시
    if DART_API_KEY:
        try:
            ltd = _latest_trading_day()
            rows = _dart_list(ltd, now_kst().strftime("%Y%m%d"), max_pages=3)
            res = [r for r in rows if r.get("stock_code") and _classify_disclosure(r.get("report_nm")) == "result"][:5]
            if res:
                el = []
                for r in res:
                    nums = fetch_earnings_numbers(r["rcept_no"])
                    num_txt = f" · {_earnings_numbers_text(nums)}" if nums else ""
                    el.append(f"· {r['corp_name']}{num_txt}\n   🔗 {_stock_link(r['stock_code'])}")
                lines.append("📊 최근 실적 공시\n" + "\n".join(el))
        except Exception as e:
            print(f"[brief:candidates] 실적 공시 실패: {e}")
    # ④ 전일 외국인 순매수 TOP5
    try:
        ltd = _latest_trading_day()
        df = _krx_df(stock.get_market_net_purchases_of_equities, ltd, ltd, "ALL", "외국인")
        if df is not None and not df.empty:
            col = "순매수거래대금" if "순매수거래대금" in df.columns else df.columns[-1]
            top = df.sort_values(col, ascending=False).head(5)
            fl = []
            for code, rr in top.iterrows():
                nm = rr.get("종목명") if isinstance(rr.get("종목명"), str) else _safe_ticker_name(code)
                fl.append(f"· {nm} {_fmt_eok(float(rr[col]) / 1e8)} 🔗 {_stock_link(code)}")
            lines.append("🌏 전일 외국인 순매수 TOP5\n" + "\n".join(fl))
    except Exception as e:
        print(f"[brief:candidates] 외국인 순매수 실패: {e}")
    return "\n\n".join(lines)


def build_premarket_brief():
    """📋 장전 리포트 = 어제 수급 한 줄 + 오늘 주요 일정 + 증권가 목표가"""
    now = now_kst()
    parts = [f"📋 장전 리포트 ({_kr_date(now.strftime('%Y-%m-%d'))})\n━━━━━━━━━━━━"]
    # 💰 어제 수급 한 줄
    try:
        rows = (
            supabase.table("market_flow_trend").select("*").eq("market", "KOSPI")
            .order("trade_date", desc=True).limit(1).execute().data
        )
        if rows:
            r = rows[0]
            f = float(r.get("foreign_amt") or 0)
            st = _streak("KOSPI", "foreign_amt", f)
            parts.append(
                f"💰 전일({_date_head(str(r['trade_date'])[:10])}) 코스피 수급\n"
                f"외국인 {_fmt_eok(f)}" + (f" ⟨{st}거래일 연속 순{'매수' if f > 0 else '매도'}⟩" if st >= 2 else "")
                + f" · 기관 {_fmt_eok(float(r.get('institution') or 0))} · 개인 {_fmt_eok(float(r.get('individual') or 0))}"
            )
    except Exception as e:
        print(f"[brief:premarket] 수급 실패: {e}")
    # 🗓 오늘 일정
    try:
        cal = fetch_calendar(1)
        todays = [e for e in cal["events"] if e["date"] == cal["start"]]
        parts.append("🗓 오늘 주요 일정\n" + ("\n".join(_event_line(e).split("\n")[0] for e in todays[:8]) if todays else "· 큰 일정 없음"))
    except Exception as e:
        print(f"[brief:premarket] 일정 실패: {e}")
    # 📌 오늘 볼 만한 후보 (데이터로 고른 것 — 추천 아님, 링크로 바로 종목분석)
    try:
        parts.append(_build_watch_candidates())
    except Exception as e:
        print(f"[brief:premarket] 후보 실패: {e}")
    # 🎯 증권가 목표가
    text, ref = build_targets_brief()
    if text:
        parts.append(text)
    return "\n\n".join(parts), ref


# ── 🚀 아침 상승 랭킹 (09:00~10:00) ──
# 거래대금 상위·상승률 상위에서 후보를 모은 뒤, "시가 대비 상승률"로 다시 줄 세움(갭상승 말고 장 시작 후
# 실제로 끌어올린 종목). 상위 10개는 09:00부터의 분봉으로 근사 세력평단·1시간 고점을 계산해서
# 지금 가격이 평단·고점 대비 어디쯤인지(눌림목 여지) 같이 보여줌.
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://jm-tracker.vercel.app")
_ETF_NAME_RE = re.compile(r"KODEX|TIGER|KBSTAR|RISE|ACE|SOL|HANARO|ARIRANG|KOSEF|PLUS|KIWOOM|TIMEFOLIO|ETN|레버리지|인버스|선물|스팩")


def _kis_quote(code: str) -> dict:
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "FHKST01010100",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
    )
    o = data.get("output") or {}
    f = lambda k: float(o.get(k) or 0)
    return {"price": f("stck_prpr"), "open": f("stck_oprc"), "high": f("stck_hgpr"),
            "pct": f("prdy_ctrt"), "value_eok": f("acml_tr_pbmn") / 1e8}


def _kis_rank_candidates() -> dict:
    """거래대금 상위 + 상승률 상위 종목코드 → 이름"""
    cands = {}
    try:
        d = kis_get(
            "/uapi/domestic-stock/v1/quotations/volume-rank",
            "FHPST01710000",
            {
                "FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171", "FID_INPUT_ISCD": "0000",
                "FID_DIV_CLS_CODE": "1", "FID_BLNG_CLS_CODE": "3", "FID_TRGT_CLS_CODE": "111111111",
                "FID_TRGT_EXLS_CLS_CODE": "0000000000", "FID_INPUT_PRICE_1": "", "FID_INPUT_PRICE_2": "",
                "FID_VOL_CNT": "", "FID_INPUT_DATE_1": "",
            },
        )
        for r in d.get("output") or []:
            c = r.get("mksc_shrn_iscd") or r.get("stck_shrn_iscd")
            if c:
                cands[c] = r.get("hts_kor_isnm") or c
    except Exception as e:
        print(f"[brief:rally] 거래대금 순위 실패: {e}")
    try:
        d = kis_get(
            "/uapi/domestic-stock/v1/ranking/fluctuation",
            "FHPST01700000",
            {
                "fid_cond_mrkt_div_code": "J", "fid_cond_scr_div_code": "20170", "fid_input_iscd": "0000",
                "fid_rank_sort_cls_code": "0", "fid_input_cnt_1": "0", "fid_prc_cls_code": "1",
                "fid_input_price_1": "", "fid_input_price_2": "", "fid_vol_cnt": "", "fid_trgt_cls_code": "0",
                "fid_trgt_exls_cls_code": "0", "fid_div_cls_code": "0", "fid_rsfl_rate1": "", "fid_rsfl_rate2": "",
            },
        )
        for r in d.get("output") or []:
            c = r.get("stck_shrn_iscd") or r.get("mksc_shrn_iscd")
            if c:
                cands.setdefault(c, r.get("hts_kor_isnm") or c)
    except Exception as e:
        print(f"[brief:rally] 상승률 순위 실패: {e}")
    return cands


def build_rally_brief(top_n: int = 20, min_value_eok: float = 50):
    now = now_kst()
    today = now.strftime("%Y-%m-%d")
    cands = _kis_rank_candidates()
    etf = _get_krx_etf_codes()
    rows = []
    for code, name in list(cands.items())[:90]:  # 20개를 채울 수 있게 후보를 넉넉히
        if code in etf or _ETF_NAME_RE.search(name or "") or _is_preferred_stock_name(name):
            continue
        try:
            q = _kis_quote(code)
        except Exception:
            continue
        time.sleep(0.06)  # 한투 초당 호출 한도 여유
        if q["open"] <= 0 or q["price"] <= 0 or q["value_eok"] < min_value_eok:
            continue
        q.update({"code": code, "name": name, "from_open": (q["price"] / q["open"] - 1) * 100})
        rows.append(q)
    rows = sorted([r for r in rows if r["from_open"] > 0], key=lambda r: -r["from_open"])[:top_n]
    if not rows:
        return None, None

    lines = [f"🚀 아침 상승 랭킹 ({now.month}/{now.day} 09:00~{now.strftime('%H:%M')} 기준)\n━━━━━━━━━━━━",
             "시가 대비 상승률 순 · 거래대금 50억 이상 · ETF·스팩·우선주 제외"]
    for i, r in enumerate(rows, start=1):
        extra = ""
        try:
            candles, latest, *_ = fetch_minute_candles_paginated(r["code"], "J", "09:00")
            if latest and latest != now.strftime("%Y%m%d"):
                return None, None  # 오늘 분봉이 없음 = 휴장
            est = estimate_force_avg([{"date": c["time"], "open": c["open"], "close": c["close"], "volume": c["volume"]} for c in candles])
            hi = max(c["high"] for c in candles) if candles else r["high"]
            avg = est[-1]["avg_price"] if est else 0
            if avg:
                extra = (f"\n   세력평단(근사) {avg:,.0f} · 평단 대비 {(r['price'] / avg - 1) * 100:+.1f}%"
                         f" · 장중 고점 대비 {(r['price'] / hi - 1) * 100:+.1f}%")
        except Exception as e:
            print(f"[brief:rally] {r['code']} 분봉 실패: {e}")
        lines.append(
            f"\n{i}. {r['name']} 시가대비 {r['from_open']:+.1f}% (전일비 {r['pct']:+.1f}%) · {_fmt_eok(r['value_eok'], signed=False)}"
            f"{extra}\n   🔗 {FRONTEND_URL}/stock-analysis?code={r['code']}"
        )
        time.sleep(0.1)
    lines.append("\n※ 평단 대비 +가 작고 고점 대비 -가 적당하면 눌림목 후보 — 링크로 분봉·세력평단 확인")
    return "\n".join(lines), today


def _is_trading_today() -> bool:
    """장중에도 쓸 수 있는 휴장일 확인 — 삼성전자 오늘 분봉이 있으면 장 열린 날"""
    try:
        rows = _kis_minute_page("005930", "J", now_kst().strftime("%H%M%S"))
        return bool(rows) and max(r.get("stck_bsop_date", "") for r in rows) == now_kst().strftime("%Y%m%d")
    except Exception:
        return now_kst().weekday() < 5


def build_theme_am_brief():
    text, ref = build_theme_brief(title="🌅 오전장 테마 이슈")
    if text:
        text += "\n\n※ 오전 10시 기준이라 20:00 마감 테마 이슈와 비교하면 장중 변동성(지속·소멸)을 볼 수 있어요."
    return text, ref


BRIEF_BUILDERS = {
    "rally": build_rally_brief,
    "theme_am": build_theme_am_brief,
    "morning": build_morning_brief,
    "targets": build_premarket_brief,
    "schedule": build_schedule_brief,
    "flow": build_flow_brief,
    "theme": build_theme_brief,
}


def _run_telegram_briefs():
    """daily_jobs_worker가 5분마다 부름 — 정해진 시각이 지났고 아직 안 보낸 브리핑만 보냄"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    now = now_kst()
    wd = now.weekday()  # 0=월 ... 6=일
    for kind, (h, m) in BRIEF_SCHEDULE.items():
        if (now.hour, now.minute) < (h, m) or now.hour >= 23:
            continue
        if kind == "morning" and wd not in (1, 2, 3, 4, 5):  # 화~토 아침 = 미국 월~금장
            continue
        if kind != "morning" and wd >= 5:
            continue
        if kind == "theme_am" and not _is_trading_today():
            continue
        if kind in ("flow", "theme"):  # 일정(14:00)은 장중이라 KRX 오늘 확정 전이어도 평일이면 보냄
            try:
                if _latest_trading_day() != now.strftime("%Y%m%d"):
                    continue  # 오늘 휴장
            except Exception:
                continue
        try:
            # 기준 날짜를 먼저 정해서 이미 보냈는지 확인 (모닝·목표가는 내용 날짜, 나머지는 오늘)
            if kind in ("flow", "theme", "schedule", "theme_am", "rally") and _brief_already_sent(kind, now.strftime("%Y-%m-%d")):
                continue
            text, ref = BRIEF_BUILDERS[kind]()
            if not text or not ref or _brief_already_sent(kind, ref):
                continue
            if send_telegram(text):
                _mark_brief_sent(kind, ref)
                print(f"[brief] {kind} 발송 완료 ({ref})")
        except Exception as e:
            print(f"[brief] {kind} 만들기 실패: {e}")


@app.get("/api/brief")
def brief_preview_endpoint(kind: str = "flow", send: bool = False):
    """브리핑 미리보기 — kind: morning/targets(장전 리포트)/schedule/flow/theme. send=true면 텔레그램으로도 보냄(기록 안 남김)"""
    if kind not in BRIEF_BUILDERS:
        return {"ok": False, "error": f"kind는 {'/'.join(BRIEF_BUILDERS)} 중 하나예요."}
    try:
        text, ref = BRIEF_BUILDERS[kind]()
        sent = send_telegram(text) if (send and text) else False
        return {"ok": True, "kind": kind, "ref_date": ref, "sent": sent, "text": text}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 🎯 증권가 목표가 도달 알림
# ──────────────────────────────────────────────────────────────────────────
# 관심종목(watchlist)의 "정규장 종가"가 증권사 목표가에 다가가거나 넘으면 텔레그램으로 알림.
#   · 근접: 최근 3개월 평균 목표가의 95% 이상
#   · 도달: 평균 목표가 이상
#   · 돌파: 증권사 중 가장 높은 목표가 이상
# 정규장 마감(15:30) 뒤 15:40에 하루 1번 종가로 확인 (장중 흔들림에 알림이 오락가락하지 않게 — 2026-10-10 요청).
# 같은 종목·같은 단계·같은 목표가로는 한 번만 보냄
# (증권사가 목표가를 올리면 새 목표가 기준으로 다시 알림).
# 알림 전용 봇을 따로 쓰고 싶으면 Render 환경변수 TARGET_ALERT_BOT_TOKEN / TARGET_ALERT_CHAT_ID
# (없으면 기본 봇 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID로 보냄)
# ══════════════════════════════════════════════════════════════════════════
TARGET_ALERT_BOT_TOKEN = os.environ.get("TARGET_ALERT_BOT_TOKEN")
TARGET_ALERT_CHAT_ID = os.environ.get("TARGET_ALERT_CHAT_ID")


def _kis_current_price(code: str):
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            "FHKST01010100",
            {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
        )
        out = data.get("output") or {}
        p = float(out.get("stck_prpr") or 0)
        return p if p > 0 else None
    except Exception as e:
        print(f"[target_reach] {code} 현재가 조회 실패: {e}")
        return None


def evaluate_target_reach(code: str, name: str) -> dict:
    rep_ = fetch_stock_reports(code)
    cutoff = (now_kst() - timedelta(days=92)).strftime("%Y-%m-%d")
    recent = [r for r in rep_.get("items", []) if r.get("target_price") and (r.get("date") or "") >= cutoff]
    if not recent:
        return {"code": code, "name": name, "status": "no_target"}
    tps = [r["target_price"] for r in recent]
    avg = round(sum(tps) / len(tps))
    top = max(tps)
    top_broker = next((r["broker"] for r in recent if r["target_price"] == top), "")
    price = _kis_current_price(code)
    if not price:
        return {"code": code, "name": name, "status": "no_price", "avg": avg, "top": top}
    level = None
    if price >= top:
        level = "break_top"
    elif price >= avg:
        level = "reach_avg"
    elif price >= avg * 0.95:
        level = "near_avg"
    return {
        "code": code, "name": name, "price": price, "avg": avg, "top": top, "top_broker": top_broker,
        "count": len(recent), "level": level, "pct_of_avg": price / avg * 100,
    }


def _target_reach_message(e: dict) -> str:
    head = {
        "near_avg": "🟡 목표가 근접",
        "reach_avg": "🎯 평균 목표가 도달",
        "break_top": "🚀 최고 목표가 돌파",
    }[e["level"]] + " (종가 기준)"
    return (
        f"{head} — {e['name']} ({e['code']})\n"
        f"종가 {e['price']:,.0f}원 · 평균 목표가의 {e['pct_of_avg']:.1f}%\n"
        f"· 3개월 평균 목표가 {e['avg']:,}원 (리포트 {e['count']}건)\n"
        f"· 최고 목표가 {e['top']:,}원 ({e['top_broker']})"
    )


def _check_target_reach():
    token = TARGET_ALERT_BOT_TOKEN or TELEGRAM_BOT_TOKEN
    chat = TARGET_ALERT_CHAT_ID or TELEGRAM_CHAT_ID
    if not token or not chat:
        return
    now = now_kst()
    today = now.strftime("%Y-%m-%d")
    # 평일 15:40~18:00 사이에 하루 1번 (마감 직후 KRX 확정이 늦으면 5분마다 다시 시도, 18시 넘으면 그날은 휴장으로 봄)
    if now.weekday() >= 5 or not ((15, 40) <= (now.hour, now.minute) < (18, 0)):
        return
    if _brief_already_sent("reach_run", today):
        return
    try:
        if _latest_trading_day() != now.strftime("%Y%m%d"):
            return  # 아직 오늘 종가 확정 전이거나 휴장일
    except Exception:
        return
    try:
        wl = supabase.table("watchlist").select("code,name").execute().data or []
    except Exception as e:
        print(f"[target_reach] 관심종목 조회 실패: {e}")
        return
    order = {"near_avg": 1, "reach_avg": 2, "break_top": 3}
    for w in wl:
        code = w.get("code")
        if not code:
            continue
        try:
            e = evaluate_target_reach(code, w.get("name") or code)
        except Exception as ex:
            print(f"[target_reach] {code} 확인 실패: {ex}")
            continue
        if not e.get("level"):
            continue
        # 같은 목표가 기준으로 이미 같은(또는 더 높은) 단계를 보냈으면 건너뜀
        ref = f"{code}:{e['avg']}:{e['top']}"
        try:
            sent = (
                supabase.table("telegram_sent_log").select("kind").like("kind", "reach_%").eq("ref_date", ref).execute().data
            ) or []
        except Exception:
            sent = []
        best_sent = max((order.get(x["kind"].replace("reach_", ""), 0) for x in sent), default=0)
        if order[e["level"]] <= best_sent:
            continue
        if send_telegram(_target_reach_message(e), token=token, chat_id=chat):
            _mark_brief_sent(f"reach_{e['level']}", ref)
        time.sleep(0.2)
    _mark_brief_sent("reach_run", today)


@app.get("/api/target-reach")
def target_reach_endpoint(code: str = "", send: bool = False):
    """목표가 도달 확인 — code를 주면 그 종목만, 비우면 관심종목 전체. send=true면 해당되는 알림을 지금 보냄(기록 안 남김)"""
    try:
        if code:
            targets = [{"code": code.strip().upper(), "name": _safe_ticker_name(code.strip().upper())}]
        else:
            targets = supabase.table("watchlist").select("code,name").execute().data or []
        results = [evaluate_target_reach(t["code"], t.get("name") or t["code"]) for t in targets]
        if send:
            for e in results:
                if e.get("level"):
                    send_telegram(
                        _target_reach_message(e),
                        token=TARGET_ALERT_BOT_TOKEN or TELEGRAM_BOT_TOKEN,
                        chat_id=TARGET_ALERT_CHAT_ID or TELEGRAM_CHAT_ID,
                    )
        return {"ok": True, "results": results}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 📊 실적 공시 알림 (DART) — 실적 발표한 종목 + 실적발표 예고·IR 개최 공시
# ──────────────────────────────────────────────────────────────────────────
# 평일 07:30~20:30 사이 10분마다 오늘 올라온 거래소 공시(DART list.json)를 확인해서,
#   📊 잠정실적·영업실적 공정공시, 매출액/손익구조 30%(15%) 이상 변경  → "실적 발표"
#   📢 결산실적공시 예고, 기업설명회(IR) 개최                       → "실적 발표 예정"
# 새로 나온 것만 모아서 한 번에 보냄 (관심종목은 ⭐로 맨 위). 이미 보낸 공시는 다시 안 보냄.
# 필요: Render 환경변수 DART_API_KEY
# ══════════════════════════════════════════════════════════════════════════
EARN_RESULT_RE = re.compile(r"잠정\)?\s*실적|영업\s*\(잠정\)\s*실적|손익구조\s*\d+%|매출액또는손익구조")
EARN_PLAN_RE = re.compile(r"결산실적공시\s*예고|기업설명회\s*\(IR\)\s*개최")
_earn_last = {"at": 0.0}


def _dart_list(bgn: str, end: str, max_pages: int = 6) -> list:
    """DART 거래소공시(I) 목록 — bgn/end: YYYYMMDD"""
    if not DART_API_KEY:
        raise RuntimeError("DART_API_KEY 미설정")
    out = []
    for page in range(1, max_pages + 1):
        res = requests.get(
            "https://opendart.fss.or.kr/api/list.json",
            params={"crtfc_key": DART_API_KEY, "bgn_de": bgn, "end_de": end, "pblntf_ty": "I",
                    "page_no": page, "page_count": 100, "sort": "date", "sort_mth": "desc"},
            timeout=15,
        )
        data = res.json()
        if data.get("status") != "000":
            break
        rows = data.get("list") or []
        out += rows
        if page >= int(data.get("total_page") or 1):
            break
    return out


def _classify_disclosure(report_nm: str):
    if EARN_RESULT_RE.search(report_nm or ""):
        return "result"
    if EARN_PLAN_RE.search(report_nm or ""):
        return "plan"
    return None


# ── 잠정실적 숫자 꺼내기 (DART 공식 "공시서류원본파일" API — document.xml) ──
# ⚠️ DART 웹 뷰어(dart.fss.or.kr) 페이지는 자동 수집을 막아둬서(robots) 쓰지 않고, OpenDART 공식 API로만 받음.
# 원문 표에서 매출액·영업이익·당기순이익 줄을 찾아 [당기, 전기, 전기대비%, 전년동기, 전년동기대비%] 순서로 읽음.
_earn_num_cache: dict = {}
_ACC_ROWS = [("매출", ("매출액", "수익(매출액)", "영업수익")), ("영업이익", ("영업이익",)), ("순이익", ("당기순이익", "지배기업소유주지분순이익", "지배기업 소유주지분 순이익"))]
_UNIT_TO_EOK = {"억원": 1.0, "백만원": 0.01, "천원": 1e-5, "원": 1e-8}


def _parse_num(t: str):
    t = (t or "").replace(",", "").replace(" ", "").strip()
    if t in ("", "-"):
        return None
    neg = t.startswith("(") and t.endswith(")") or t.startswith("△") or t.startswith("-")
    t = t.strip("()△-")
    try:
        v = float(t)
    except ValueError:
        return t if ("흑자" in t or "적자" in t) else None
    return -v if neg else v


def fetch_earnings_numbers(rcept_no: str):
    if rcept_no in _earn_num_cache:
        return _earn_num_cache[rcept_no]
    out = None
    try:
        res = requests.get(
            "https://opendart.fss.or.kr/api/document.xml",
            params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no},
            timeout=15,
        )
        if res.content[:2] != b"PK":  # zip이 아니면 오류 응답(지원 안 되는 공시 등)
            _earn_num_cache[rcept_no] = None
            return None
        with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
            raw = b"".join(zf.read(n) for n in zf.namelist())
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("euc-kr", errors="replace")
        unit_m = re.search(r"단위\s*[:：]?\s*(억원|백만원|천원|원)", text)
        mul = _UNIT_TO_EOK.get(unit_m.group(1), None) if unit_m else None
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # DART 원문은 XML이라 HTML 파서 경고가 뜨는데, 표 읽기엔 문제없음
            soup = BeautifulSoup(text, "html.parser")
        found = {}
        for tr in soup.find_all(["tr", "TR"]):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th", "te", "tu", "TD", "TH", "TE", "TU"])]
            if not cells:
                continue
            head = cells[0].replace(" ", "")
            key = next((k for k, names in _ACC_ROWS if any(head.startswith(n.replace(" ", "")) for n in names)), None)
            if not key or key in found:
                continue
            nums = [_parse_num(c) for c in cells[1:]]
            nums = [n for n in nums if n is not None]
            if not nums or not isinstance(nums[0], float):
                continue
            cur = nums[0]
            yoy = qoq = None
            if len(nums) >= 5:  # 당기, 전기, 전기대비%, 전년동기, 전년동기대비%
                qoq, yoy = nums[2], nums[4]
            elif len(nums) >= 2 and isinstance(nums[1], float) and nums[1]:  # 당기, 전기(또는 전년)
                yoy = (cur / nums[1] - 1) * 100 if nums[1] > 0 else ("흑자전환" if cur > 0 > nums[1] else None)
            found[key] = {"cur": cur * mul if mul else None, "raw": cur, "qoq": qoq, "yoy": yoy}
        out = found or None
    except Exception as e:
        print(f"[earnings] {rcept_no} 숫자 읽기 실패: {e}")
    _earn_num_cache[rcept_no] = out
    return out


def _earnings_numbers_text(nums: dict) -> str:
    parts = []
    for key, _ in _ACC_ROWS:
        v = nums.get(key)
        if not v:
            continue
        amount = _fmt_eok(v["cur"], signed=False) if v.get("cur") is not None else f"{v['raw']:,.0f}"
        if v["cur"] is not None and v["cur"] < 0:
            amount = "-" + amount
        yoy = v.get("yoy")
        if isinstance(yoy, float):
            yoy_txt = f" (YoY {yoy:+.0f}%)"
        elif isinstance(yoy, str):
            yoy_txt = f" ({yoy})"
        else:
            yoy_txt = ""
        parts.append(f"{key} {amount}{yoy_txt}")
    return " · ".join(parts)


def _disclosure_line(r: dict, watch: set) -> str:
    star = "⭐" if r.get("stock_code") in watch else ""
    mkt = {"Y": "코스피", "K": "코스닥"}.get(r.get("corp_cls"), "")
    name = re.sub(r"\s+", " ", r.get("report_nm") or "").strip()
    nums_line = ""
    if _classify_disclosure(r.get("report_nm")) == "result":
        nums = fetch_earnings_numbers(r.get("rcept_no"))
        if nums:
            nums_line = f"\n   💰 {_earnings_numbers_text(nums)}"
    return (f"{star}{r.get('corp_name')}" + (f" ({mkt})" if mkt else "") + f"\n   {name}{nums_line}"
            f"\n   🔗 https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.get('rcept_no')}")


def build_earnings_digest(days: int = 1, only_new: bool = False):
    now = now_kst()
    bgn = (now - timedelta(days=days - 1)).strftime("%Y%m%d")
    rows = _dart_list(bgn, now.strftime("%Y%m%d"))
    rows = [r for r in rows if r.get("stock_code") and _classify_disclosure(r.get("report_nm"))]
    if only_new:
        rows = [r for r in rows if not _brief_already_sent("dart_earn", r["rcept_no"])]
    try:
        watch = {w["code"] for w in (supabase.table("watchlist").select("code").execute().data or [])}
    except Exception:
        watch = set()
    results = [r for r in rows if _classify_disclosure(r["report_nm"]) == "result"]
    plans = [r for r in rows if _classify_disclosure(r["report_nm"]) == "plan"]
    key = lambda r: (r.get("stock_code") not in watch, r.get("corp_cls") not in ("Y", "K"))
    results.sort(key=key)
    plans.sort(key=key)
    if not results and not plans:
        return None, rows
    parts = [f"📊 실적 공시 알림 ({now.strftime('%m/%d %H:%M')})\n━━━━━━━━━━━━"]
    if results:
        parts.append(f"📊 실적 발표 ({len(results)}건)\n" + "\n".join(_disclosure_line(r, watch) for r in results[:25])
                     + (f"\n외 {len(results) - 25}건" if len(results) > 25 else ""))
    if plans:
        parts.append(f"📢 실적 발표 예고·IR ({len(plans)}건)\n" + "\n".join(_disclosure_line(r, watch) for r in plans[:15])
                     + (f"\n외 {len(plans) - 15}건" if len(plans) > 15 else ""))
    return "\n\n".join(parts), rows


def _check_earnings_disclosures():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not DART_API_KEY:
        return
    now = now_kst()
    if now.weekday() >= 5 or not ((7, 30) <= (now.hour, now.minute) <= (20, 30)):
        return
    if time.time() - _earn_last["at"] < 10 * 60:
        return
    _earn_last["at"] = time.time()
    try:
        text, rows = build_earnings_digest(days=1, only_new=True)
    except Exception as e:
        print(f"[earnings] 공시 확인 실패: {e}")
        return
    if text and send_telegram(text):
        for r in rows:
            _mark_brief_sent("dart_earn", r["rcept_no"])


@app.get("/api/earnings-disclosures")
def earnings_disclosures_endpoint(days: int = 3, send: bool = False):
    """최근 days일 실적 공시(발표·예고·IR) 미리보기 — send=true면 텔레그램으로도 보냄(기록 안 남김)"""
    try:
        text, rows = build_earnings_digest(days=max(1, min(days, 14)))
        sent = send_telegram(text) if (send and text) else False
        items = [
            {
                "date": f"{r['rcept_dt'][:4]}-{r['rcept_dt'][4:6]}-{r['rcept_dt'][6:8]}" if r.get("rcept_dt") else None,
                "corp_name": r.get("corp_name"),
                "stock_code": r.get("stock_code"),
                "market": {"Y": "코스피", "K": "코스닥"}.get(r.get("corp_cls"), ""),
                "report_nm": re.sub(r"\s+", " ", r.get("report_nm") or "").strip(),
                "type": _classify_disclosure(r.get("report_nm")),
                "link": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.get('rcept_no')}",
            }
            for r in rows
        ]
        return {"ok": True, "count": len(rows), "sent": sent, "items": items, "text": text or "해당 기간 실적 공시가 없어요."}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 📑 재무 상세 (DART 공식) — 수익성·안정성·현금흐름·배당
# ──────────────────────────────────────────────────────────────────────────
#   · 수익성: 분기 영업이익률·순이익률 (fetch_dart_quarterly 재사용), 최근 4분기 ROE
#   · 안정성: 부채비율·유동비율·자본총계 — 가장 최근 보고서의 재무상태표
#   · 현금흐름: 영업·투자·재무활동 현금흐름(올해 누적), 잉여현금흐름(영업CF + 투자CF, 근사)
#   · 배당: 최근 사업보고서의 주당배당금·배당수익률·배당성향 (alotMatter)
# ══════════════════════════════════════════════════════════════════════════
_fin_cache: dict = {}
BS_ACC = {
    "assets": ["자산총계", "자산합계", "총자산"],
    "liabilities": ["부채총계", "부채합계", "총부채"],
    "equity": ["자본총계", "자본합계", "총자본"],
    "cur_assets": ["유동자산"],
    "cur_liabilities": ["유동부채"],
}
# 회사마다 계정 이름이 조금씩 달라서(2026-10-12 로보티즈 부채·자본 누락 확인) 국제표준 계정코드로도 찾음
BS_IDS = {
    "assets": ["ifrs-full_Assets"],
    "liabilities": ["ifrs-full_Liabilities"],
    "equity": ["ifrs-full_Equity"],
    "cur_assets": ["ifrs-full_CurrentAssets"],
    "cur_liabilities": ["ifrs-full_CurrentLiabilities"],
}
CF_ACC = {
    "cfo": ["영업활동현금흐름", "영업활동으로인한현금흐름", "영업활동순현금흐름"],
    "cfi": ["투자활동현금흐름", "투자활동으로인한현금흐름", "투자활동순현금흐름"],
    "cff": ["재무활동현금흐름", "재무활동으로인한현금흐름", "재무활동순현금흐름"],
}


def _dart_full(corp: str, year: int, reprt_code: str):
    """전체 재무제표 — 연결(CFS) 먼저, 없으면 별도(OFS)"""
    for fs in ("CFS", "OFS"):
        res = requests.get(
            "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
            params={"crtfc_key": DART_API_KEY, "corp_code": corp, "bsns_year": str(year), "reprt_code": reprt_code, "fs_div": fs},
            timeout=15,
        )
        data = res.json()
        if data.get("status") == "000" and data.get("list"):
            return data["list"], fs
    return None, None


def _pick_acc(rows: list, sj: tuple, names: list, field: str = "thstrm_amount", ids: list = None):
    if ids:  # 계정코드가 정확히 맞는 줄이 있으면 그걸 우선
        for r in rows:
            if r.get("sj_div") in sj and r.get("account_id") in ids:
                v = _to_eok(r.get(field))
                if v is not None:
                    return v
    for r in rows:
        if r.get("sj_div") not in sj:
            continue
        nm = (r.get("account_nm") or "").replace(" ", "")
        if any(nm == n or nm.startswith(n) for n in names):
            v = _to_eok(r.get(field))
            if v is not None:
                return v
    return None


def fetch_dart_financials(stock_code: str) -> dict:
    cached = _fin_cache.get(stock_code)
    if cached and time.time() - cached[0] < 12 * 3600:
        return cached[1]
    corp = _dart_corp_code(stock_code)
    if not corp:
        raise ValueError("DART에서 이 종목을 찾지 못했어요.")

    # 가장 최근 보고서 찾기 (올해 3분기 → 반기 → 1분기 → 작년 사업보고서 …)
    y = now_kst().year
    order = [(y, "11014", "3분기"), (y, "11012", "반기"), (y, "11013", "1분기"),
             (y - 1, "11011", "사업보고서"), (y - 1, "11014", "3분기"), (y - 1, "11012", "반기")]
    rows, fs, period = None, None, None
    for yy, rc, label in order:
        try:
            rows, fs = _dart_full(corp, yy, rc)
        except Exception:
            rows = None
        if rows:
            period = f"{yy}년 {label}"
            break
        time.sleep(0.1)

    stability, cashflow = {}, {}
    if rows:
        for k, names in BS_ACC.items():
            stability[k] = _pick_acc(rows, ("BS",), names, ids=BS_IDS.get(k))
        # 하나가 빠지면 "자산 = 부채 + 자본"으로 채움
        a_, l_, e_ = stability.get("assets"), stability.get("liabilities"), stability.get("equity")
        if e_ is None and a_ is not None and l_ is not None:
            stability["equity"] = round(a_ - l_, 1)
        elif l_ is None and a_ is not None and e_ is not None:
            stability["liabilities"] = round(a_ - e_, 1)
        for k, names in CF_ACC.items():
            cashflow[k] = _pick_acc(rows, ("CF",), names)
        if stability.get("liabilities") is not None and stability.get("equity"):
            stability["debt_ratio"] = round(stability["liabilities"] / stability["equity"] * 100, 1)
        if stability.get("cur_assets") is not None and stability.get("cur_liabilities"):
            stability["current_ratio"] = round(stability["cur_assets"] / stability["cur_liabilities"] * 100, 1)
        if cashflow.get("cfo") is not None and cashflow.get("cfi") is not None:
            cashflow["fcf"] = round(cashflow["cfo"] + cashflow["cfi"], 1)

    # 수익성 (분기 실적 재사용)
    margins, ttm_roe = [], None
    try:
        q = fetch_dart_quarterly(stock_code)
        for row in q.get("quarters", []):
            rev = row.get("revenue")
            margins.append({
                "period": row["period"],
                "op_margin": round(row["op"] / rev * 100, 1) if rev and row.get("op") is not None else None,
                "net_margin": round(row["net"] / rev * 100, 1) if rev and row.get("net") is not None else None,
            })
        if q.get("ttm_net") is not None and stability.get("equity"):
            ttm_roe = round(q["ttm_net"] / stability["equity"] * 100, 1)
    except Exception as e:
        print(f"[financials] 수익성 실패: {e}")

    # 배당 (작년 사업보고서 → 재작년)
    dividend = None
    for yy in (y - 1, y - 2):
        try:
            res = requests.get(
                "https://opendart.fss.or.kr/api/alotMatter.json",
                params={"crtfc_key": DART_API_KEY, "corp_code": corp, "bsns_year": str(yy), "reprt_code": "11011"},
                timeout=10,
            )
            data = res.json()
        except Exception:
            continue
        if data.get("status") != "000":
            continue
        d = {"year": yy}
        for r in data.get("list") or []:
            se = (r.get("se") or "").replace(" ", "")
            kind = r.get("stock_knd") or ""
            val = (r.get("thstrm") or "").replace(",", "").strip()
            try:
                v = float(val)
            except ValueError:
                continue
            if "주당현금배당금" in se and ("보통" in kind or not kind) and "dps" not in d:
                d["dps"] = v
            elif "현금배당수익률" in se and ("보통" in kind or not kind) and "yield" not in d:
                d["yield"] = v
            elif "현금배당성향" in se and "payout" not in d:
                d["payout"] = v
        dividend = d
        break

    result = {
        "ok": True, "code": stock_code, "fs": "연결" if fs == "CFS" else "별도" if fs == "OFS" else None,
        "period": period, "unit": "억원",
        "stability": stability, "cashflow": cashflow, "margins": margins[-8:], "ttm_roe": ttm_roe, "dividend": dividend,
    }
    _fin_cache[stock_code] = (time.time(), result)
    return result


@app.get("/api/dart-financials")
def dart_financials_endpoint(code: str):
    """재무 상세 — 수익성(이익률·ROE)·안정성(부채비율·유동비율)·현금흐름·배당 (DART 공식)"""
    try:
        return fetch_dart_financials((code or "").strip().upper())
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 🌡 시장심리 — 고객예탁금·신용융자 잔고·반대매매 (금융투자협회 통계)
# ──────────────────────────────────────────────────────────────────────────
# ⚠️ 출처 응답 모양을 이 작업 환경에서 직접 확인하지 못해서 후보 2곳을 차례로 시도함.
# 안 나오면 /api/market-sentiment-debug 로 실제 응답을 보고 맞춰야 함. 하루 1번(18시 이후) 갱신되는 통계라 6시간 보관.
# ══════════════════════════════════════════════════════════════════════════
_sentiment_cache = {"at": 0.0, "data": None}


def _num_kr(t):
    t = str(t or "").replace(",", "").strip()
    try:
        return float(t)
    except ValueError:
        return None


def _sentiment_from_naver() -> list:
    """네이버 증시자금동향 (옛 페이지) — 날짜·고객예탁금·신용잔고 (단위 억원)"""
    res = requests.get("https://finance.naver.com/sise/sise_deposit.naver", headers=_naver_headers(), timeout=10)
    html = _decode_html(res)
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 4 or not re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", cells[0]):
            continue
        nums = [_num_kr(c) for c in cells[1:]]
        rows.append({
            "date": f"20{cells[0][:2]}-{cells[0][3:5]}-{cells[0][6:8]}",
            "deposit": nums[0] if nums else None,
            "credit": nums[2] if len(nums) > 2 else None,
        })
    return rows


def _kofia_rows(obj_nm: str, days: int = 90) -> list:
    end = now_kst()
    start = end - timedelta(days=days)
    res = requests.post(
        "https://freesis.kofia.or.kr/meta/getMetaDataList.do",
        json={"dmSearch": {"tmpV40": "1000000", "tmpV41": "1", "tmpV1": "D", "tmpV45": start.strftime("%Y%m%d"),
                           "tmpV46": end.strftime("%Y%m%d"), "OBJ_NM": obj_nm}},
        headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
        timeout=12,
    )
    return res.json().get("ds1") or []


def _sentiment_from_kofia() -> list:
    """
    금융투자협회 FreeSIS (2026-10-12 디버그로 확인, 단위 백만원 → 억원으로 바꿈)
    ① 증시자금추이(STATSCU0100000060BO): TMPV2 투자자예탁금 · TMPV5 위탁매매 미수금 ·
       TMPV6 미수금 대비 실제 반대매매 금액 · TMPV7 반대매매 비중(%)
    ② 신용공여 잔고추이(STATSCU0100000070BO): TMPV2 신용거래융자 합계 (⚠️ 칸 위치 확인 필요 — 크기로 검증)
    """
    by_date = {}
    for r in _kofia_rows("STATSCU0100000060BO"):
        d = str(r.get("TMPV1") or "")
        if len(d) != 8:
            continue
        dep, misu, forced = _num_kr(r.get("TMPV2")), _num_kr(r.get("TMPV5")), _num_kr(r.get("TMPV6"))
        by_date[d] = {
            "date": f"{d[:4]}-{d[4:6]}-{d[6:]}",
            "deposit": round(dep / 100) if dep is not None else None,
            "misu": round(misu / 100) if misu is not None else None,
            "forced_sale": round(forced / 100, 1) if forced is not None else None,
            "forced_ratio": _num_kr(r.get("TMPV7")),
            "credit": None,
        }
    try:
        for r in _kofia_rows("STATSCU0100000070BO"):
            d = str(r.get("TMPV1") or "")
            v = _num_kr(r.get("TMPV2"))  # 신용거래융자 합계 (2026-10-12 확인: TMPV3 코스피 · TMPV4 코스닥)
            if d in by_date and v is not None:
                eok = v / 100
                if 50_000 <= eok <= 1_000_000:  # 5조~100조 사이일 때만 신용융자로 인정 (칸이 틀리면 안 씀)
                    by_date[d]["credit"] = round(eok)
                    k, q = _num_kr(r.get("TMPV3")), _num_kr(r.get("TMPV4"))
                    by_date[d]["credit_kospi"] = round(k / 100) if k is not None else None
                    by_date[d]["credit_kosdaq"] = round(q / 100) if q is not None else None
    except Exception as e:
        print(f"[sentiment] 신용잔고 실패: {e}")
    return list(by_date.values())


def fetch_market_sentiment() -> dict:
    c = _sentiment_cache
    if c["data"] and time.time() - c["at"] < 6 * 3600:
        return c["data"]
    rows, source, errors = [], None, []
    for label, fn in (("금융투자협회", _sentiment_from_kofia), ("네이버 증시자금동향", _sentiment_from_naver)):
        try:
            rows = fn()
            if rows:
                source = label
                break
        except Exception as e:
            errors.append(f"{label}: {e}")
    rows = sorted([r for r in rows if r.get("date")], key=lambda r: r["date"])[-60:]
    data = {"ok": True, "source": source, "rows": rows, "errors": errors}
    if rows:
        c.update({"at": time.time(), "data": data})
    return data


@app.get("/api/market-sentiment")
def market_sentiment_endpoint():
    """시장심리 — 고객예탁금·신용융자 잔고·반대매매 (최근 60일, 억원)"""
    try:
        return fetch_market_sentiment()
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/market-sentiment-debug")
def market_sentiment_debug():
    out = {}
    for label, fn in (("kofia", _sentiment_from_kofia), ("naver", _sentiment_from_naver)):
        try:
            rows = fn()
            out[label] = {"count": len(rows), "sample": rows[-3:]}
        except Exception as e:
            out[label] = {"error": str(e)}
    try:
        out["credit_raw_top3"] = _kofia_rows("STATSCU0100000070BO", days=10)[:3]
    except Exception as e:
        out["credit_raw"] = {"error": str(e)}
    return out


# ══════════════════════════════════════════════════════════════════════════
# 🇺🇸 미국 핵심 지표 결과표 (고용·실업률·CPI·PCE) — FRED 공식 값
# ──────────────────────────────────────────────────────────────────────────
# 예전 매크로 화면 표(PC 프로그램이 채우던 econ_calendar)를 서버가 직접 채우는 방식으로 대체.
# 실제값·이전값은 FRED 관측치로 계산하고, 다음 발표일은 FRED 발표 일정(45일 앞까지)에서 찾음.
# ══════════════════════════════════════════════════════════════════════════
US_INDICATORS = [
    # (키, 이름, FRED 시리즈, 계산 방식, FRED 발표 이름, 단위)
    ("nfp", "비농업 고용 증감", "PAYEMS", "diff", "Employment Situation", "만명"),
    ("unrate", "실업률", "UNRATE", "level", "Employment Situation", "%"),
    ("cpi", "CPI (전년비)", "CPIAUCSL", "yoy", "Consumer Price Index", "%"),
    ("core_cpi", "근원 CPI (전년비)", "CPILFESL", "yoy", "Consumer Price Index", "%"),
    ("pce", "PCE 물가 (전년비)", "PCEPI", "yoy", "Personal Income and Outlays", "%"),
    ("core_pce", "근원 PCE (전년비)", "PCEPILFE", "yoy", "Personal Income and Outlays", "%"),
]
_us_ind_cache = {"at": 0.0, "data": None}


def _fred_obs(series: str, limit: int = 15) -> list:
    res = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={"series_id": series, "api_key": FRED_API_KEY, "file_type": "json", "sort_order": "desc", "limit": limit},
        timeout=20,
    )
    res.raise_for_status()
    out = []
    for o in res.json().get("observations", []):
        try:
            out.append((o["date"], float(o["value"])))
        except (ValueError, KeyError):
            continue
    return out  # 최신순


def fetch_us_indicators() -> dict:
    c = _us_ind_cache
    if c["data"] and time.time() - c["at"] < 3 * 3600:
        return c["data"]
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY 미설정")
    # 다음 발표일 (45일 앞까지)
    now = now_kst()
    start = now.strftime("%Y-%m-%d")
    end = (now + timedelta(days=45)).strftime("%Y-%m-%d")
    next_release = {}
    try:
        for r in _fred_release_dates(start, end):
            nm = (r.get("release_name") or "").lower()
            for _, _, _, _, rel, _ in US_INDICATORS:
                if rel.lower() in nm and r["date"] >= start and rel not in next_release:
                    et = next((t for k, _, t, *_ in US_RELEASES if k == rel), "08:30")
                    kst = _us_to_kst(r["date"], et)
                    next_release[rel] = kst.strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        print(f"[us_indicators] 다음 발표일 실패: {e}")

    rows = []
    for key, name, series, mode, rel, unit in US_INDICATORS:
        try:
            obs = _fred_obs(series)
        except Exception as e:
            rows.append({"key": key, "name": name, "error": str(e)})
            continue
        if len(obs) < 14:
            continue
        (d0, v0), (d1, v1) = obs[0], obs[1]
        if mode == "diff":  # 만 명 단위 증감 (FRED는 천 명)
            actual, prev = round((v0 - v1) / 10, 1), round((v1 - obs[2][1]) / 10, 1)
        elif mode == "yoy":
            actual = round((v0 / obs[12][1] - 1) * 100, 1)
            prev = round((v1 / obs[13][1] - 1) * 100, 1)
        else:
            actual, prev = v0, v1
        rows.append({
            "key": key, "name": name, "ref_month": d0[:7], "actual": actual, "prev": prev,
            "change": round(actual - prev, 2), "unit": unit, "next_release": next_release.get(rel),
        })
        time.sleep(0.2)
    data = {"ok": True, "rows": rows}
    c.update({"at": time.time(), "data": data})
    return data


@app.get("/api/us-indicators")
def us_indicators_endpoint():
    """미국 핵심 지표 결과 — 비농업 고용·실업률·CPI·근원CPI·PCE·근원PCE (실제·이전·다음 발표일)"""
    try:
        return fetch_us_indicators()
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/ping")
def ping():
    return {"ok": True}
@app.get("/api/stock-ohlcv")
def stock_ohlcv_endpoint(code: str, start: str, end: str):
    """
    목표가 매핑(패턴분석) 캔들차트용 — 정밀 일봉 시가/고가/저가/종가/거래량.
    pykrx(KRX 공식 데이터)로 가져오므로 키움/한투 로그인과 무관하게 항상 정확함.
    start/end: "YYYY-MM-DD" 형식.
    """
    if stock is None:
        return {"ok": False, "error": "pykrx를 사용할 수 없습니다 (서버 기동 시 로그인에 실패했을 수 있어요, 잠시 후 다시 시도해주세요)."}
    try:
        start_ymd = start.replace("-", "")
        end_ymd = end.replace("-", "")
        df = _krx_df(stock.get_market_ohlcv_by_date, start_ymd, end_ymd, code)
        if df.empty:
            return {"ok": False, "error": f"{code} 종목의 {start}~{end} 구간 일봉 데이터가 없습니다."}

        candles = []
        for date_idx, row in df.iterrows():
            candles.append(
                {
                    "date": date_idx.strftime("%Y-%m-%d"),
                    "open": float(row.get("시가", 0)),
                    "high": float(row.get("고가", 0)),
                    "low": float(row.get("저가", 0)),
                    "close": float(row.get("종가", 0)),
                    "volume": float(row.get("거래량", 0)),
                }
            )
        return {"ok": True, "code": code, "candles": candles}
    except Exception as e:
        return {"ok": False, "error": str(e)}
# ── 분봉 페이지네이션 ───────────────────────────────────────────────────────
# 한투 "주식당일분봉조회"(FHKST03010200)는 한 번 호출에 최대 30개 분봉만 주고,
# FID_INPUT_HOUR_1(HHMMSS) "이전" 시각의 분봉을 최신순으로 돌려줌. 그래서 응답에서
# 가장 오래된 시각을 커서로 삼아 반복 호출하며 장 시작(KRX 09:00 / NXT 08:00)까지 이어붙임.
_MINUTE_MAX_PAGES = 40  # NXT 08:00~20:00(720분) ÷ 30 = 24페이지 + 여유. 무한루프 방지용 상한


def _kis_minute_page(code: str, market: str, cursor_hhmmss: str) -> list:
    """분봉 한 페이지(최대 30개, 최신순) 조회. 일시적 오류는 최대 3번까지 재시도."""
    last_err = None
    for attempt in range(3):
        try:
            data = kis_get(
                "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
                tr_id="FHKST03010200",
                params={
                    "FID_ETC_CLS_CODE": "",
                    "FID_COND_MRKT_DIV_CODE": market,  # J:KRX 정규장, NX:NXT, UN:통합
                    "FID_INPUT_ISCD": code,
                    "FID_INPUT_HOUR_1": cursor_hhmmss,
                    "FID_PW_DATA_INCU_YN": "Y",
                },
            )
            if data.get("rt_cd") == "0":
                return data.get("output2") or []
            last_err = f"KIS API 오류: {data.get('msg1', '알 수 없는 오류')}"
        except Exception as e:
            last_err = str(e)
        time.sleep(0.5 * (attempt + 1))
    raise ValueError(last_err)


def fetch_minute_candles_paginated(code: str, market: str = "J", start_hhmm: str = ""):
    """
    반복 호출로 당일 분봉을 start_hhmm(기본 J=09:00, NX/UN=08:00)까지 이어붙여 반환.
    반환: (candles[오래된 순], latest_date, pages, complete, warning)
      complete=True  → 시작 시각(또는 그날 첫 분봉)까지 다 모았음
      complete=False → 상한/오류/진전 없음으로 중간에 멈춤 (warning에 이유)
    """
    if not start_hhmm:
        start_hhmm = "09:00" if market == "J" else "08:00"
    stop = start_hhmm.replace(":", "")[:4] + "00"  # HHMMSS

    cursor = now_kst().strftime("%H%M%S")
    by_hour: dict = {}  # "HHMMSS" -> 원본 row (중복 제거용)
    latest_date = ""
    pages = 0
    complete = False
    warning = None

    for _ in range(_MINUTE_MAX_PAGES):
        try:
            output2 = _kis_minute_page(code, market, cursor)
        except Exception as e:
            if not by_hour:
                raise  # 첫 페이지부터 실패하면 그대로 에러
            warning = f"{pages + 1}번째 호출에서 실패해 그때까지 모은 데이터만 반환: {e}"
            break
        pages += 1

        if not output2:
            if not by_hour:
                raise ValueError(f"{code}에 대한 분봉 데이터가 없습니다. (market={market})")
            complete = True  # 더 이전 데이터가 없음 = 그날 첫 분봉까지 도달
            break

        # 시:분만 보고 날짜를 구분 안 하면 이전 거래일 데이터가 섞임 → 첫 페이지의 가장 최근 날짜만 사용
        if not latest_date:
            latest_date = max(r.get("stck_bsop_date", "") for r in output2)
        page = [
            r
            for r in output2
            if r.get("stck_bsop_date") == latest_date and len(r.get("stck_cntg_hour", "")) == 6
        ]
        if not page:
            complete = True  # 이전 거래일 데이터만 남음 = 오늘 첫 분봉을 이미 지남
            break

        new_count = 0
        for r in page:
            if r["stck_cntg_hour"] not in by_hour:
                by_hour[r["stck_cntg_hour"]] = r
                new_count += 1

        if len(page) < len(output2):
            complete = True  # 이번 페이지에 이전 거래일이 섞여 있음 = 오늘 첫 분봉 도달
            break

        oldest = min(r["stck_cntg_hour"] for r in page)
        if oldest <= stop:
            complete = True  # 요청한 시작 시각까지 도달
            break
        if new_count == 0:
            warning = "커서를 옮겨도 새 분봉이 없어 중간에 멈췄어요."
            break

        # 다음 커서 = 가장 오래된 분봉 시각 - 1초 (같은 분봉이 또 오더라도 by_hour로 걸러짐)
        cursor = (datetime.strptime(oldest, "%H%M%S") - timedelta(seconds=1)).strftime("%H%M%S")
        time.sleep(0.15)  # 한투 초당 호출 한도 여유
    else:
        warning = f"최대 {_MINUTE_MAX_PAGES}페이지에서 멈췄어요 (시작 시각까지 못 닿았을 수 있음)."

    candles = []
    for hour in sorted(h for h in by_hour if h >= stop):
        r = by_hour[hour]
        candles.append(
            {
                "time": f"{hour[:2]}:{hour[2:4]}",
                "open": float(r.get("stck_oprc", 0)),
                "high": float(r.get("stck_hgpr", 0)),
                "low": float(r.get("stck_lwpr", 0)),
                "close": float(r.get("stck_prpr", 0)),
                "volume": float(r.get("cntg_vol", 0)),
            }
        )
    if not candles:
        raise ValueError(f"{code}에 대한 {start_hhmm} 이후 분봉 데이터가 없습니다. (market={market})")
    return candles, latest_date, pages, complete, warning


@app.get("/api/stock-minute-ohlcv")
def stock_minute_ohlcv_endpoint(code: str, market: str = "J", start_time: str = ""):
    """
    종목분석/패턴분석 분봉 캔들차트용 — 당일 분봉 시가/고가/저가/종가/거래량.
    한국투자증권 API(FHKST03010200) 기반, PC(키움) 없이 클라우드에서 바로 조회됨.
    한 번 호출에 30개만 주는 API라서, 커서(FID_INPUT_HOUR_1)를 옮겨가며 반복 호출해
    장 시작(J=09:00, NX/UN=08:00)부터 현재까지 이어붙여서 반환함.
    market: "J"(KRX 정규장, 기본값) / "NX"(NXT) / "UN"(통합)
    start_time: "HH:MM" — 비우면 시장별 기본 시작 시각
    응답의 complete=false면 시작 시각까지 다 못 모은 것(warning에 이유).
    ⚠️ 당일 하루치만 제공됨(한투 API 자체가 "당일 분봉조회"라 과거 날짜는 안 됨).
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    if market not in ("J", "NX", "UN"):
        return {"ok": False, "error": "market은 J / NX / UN 중 하나여야 해요."}
    if start_time and not re.fullmatch(r"\d{2}:?\d{2}", start_time):
        return {"ok": False, "error": "start_time은 HH:MM 형식이어야 해요. (예: 09:00)"}
    try:
        start_hhmm = start_time.replace(":", "")
        start_hhmm = f"{start_hhmm[:2]}:{start_hhmm[2:4]}" if start_hhmm else ""
        candles, latest_date, pages, complete, warning = fetch_minute_candles_paginated(code, market, start_hhmm)
        result = {
            "ok": True,
            "code": code,
            "market": market,
            "date": latest_date,
            "count": len(candles),
            "pages": pages,
            "complete": complete,
            "candles": candles,
        }
        if warning:
            result["warning"] = warning
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}
@app.get("/api/naver-industry-debug")
def naver_industry_debug(no: str = "", path: str = "stocks/industry"):
    """
    네이버가 2026-09에 finance.naver.com → stock.naver.com(Next.js)로 개편하면서
    기존 HTML 스크래핑(테마/업종 목록)이 다 깨짐. 화면이 실제로 쓰는 새 JSON API
    (m.stock.naver.com/api/{path})의 실제 응답 모양을 먼저 확인하기 위한 디버그용
    엔드포인트. path로 다른 후보 경로도 찔러볼 수 있음 (예: stocks/theme).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://m.stock.naver.com/",
    }
    try:
        base = f"https://m.stock.naver.com/api/{path}"
        if no:
            url = f"{base}/{no}"
        else:
            url = base
        res = requests.get(url, headers=headers, params={"page": 1, "pageSize": 10}, timeout=10)

        return {
            "ok": True,
            "requested_url": res.url,
            "status_code": res.status_code,
            "raw_text_preview": res.text[:3000],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


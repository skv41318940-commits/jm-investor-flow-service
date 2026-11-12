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


_KRX_LOCK = __import__("threading").Lock()
_KRX_LAST = [0.0]
_KRX_GAP = 0.6  # KRX 요청 사이 최소 간격(초) — 한꺼번에 몰아 보내면 KRX가 우리 서버를 잠깐 막음 (2026-10-30)


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
            # 🚦 KRX는 한 번에 하나씩, 0.6초 간격으로만 (꽃게 검색기 등이 동시에 몰아 보내서 막혔던 문제)
            # 자물쇠는 '출발 간격'만 맞추고 바로 풂 — 예전엔 KRX가 느릴 때 자물쇠를 쥔 채 기다려서
            # 다른 요청들이 줄줄이 막혀 서버 전체(매크로 등)가 멈췄음 (2026-10-30)
            with _KRX_LOCK:
                gap = _KRX_GAP - (time.time() - _KRX_LAST[0])
                if gap > 0:
                    time.sleep(gap)
                _KRX_LAST[0] = time.time()
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
    allow_methods=["GET", "POST"],  # ⭐ 관심종목 넣기·옮기기(POST)가 막혀서 "서버 연결 실패"가 났음 (2026-11-01)
    allow_headers=["*"],
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# 🔒 결사대 공유 사이트 잠금 (2026-11-01 JM) — 공유 사이트(SHARE_SITE_ORIGIN)에서 오는 요청은
#    세력평단·패턴·관심종목·검색기·창구 평단·매매일지 주소를 못 쓰게 막음 (시장분석·매크로·통합수급만 허용)
SHARE_SITE_ORIGIN = (os.environ.get("SHARE_SITE_ORIGIN") or "").rstrip("/").lower()
_PRIVATE_PATHS = ("/api/stock-ohlcv", "/api/stock-minute", "/api/minute", "/api/tick-avg", "/api/query-avg", "/api/pattern",
                  "/api/jmw", "/api/crab", "/api/swing", "/api/broker-pc", "/api/pc-", "/api/journal", "/api/daytrade",
                  "/api/nxt", "/api/watchlist", "/api/vp", "/api/minute-calib")


@app.middleware("http")
async def _share_site_lock(request, call_next):
    if SHARE_SITE_ORIGIN:
        origin = (request.headers.get("origin") or request.headers.get("referer") or "").lower()
        if origin.startswith(SHARE_SITE_ORIGIN) and request.url.path.startswith(_PRIVATE_PATHS):
            from fastapi.responses import JSONResponse
            return JSONResponse({"ok": False, "error": "공유 사이트에서는 볼 수 없는 기능이에요"}, status_code=403,
                                headers={"Access-Control-Allow-Origin": "*"})
    return await call_next(request)

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
        # ⏰ 정해진 시각 알림들은 _alert_clock_loop(15초)로 옮김 (2026-11-07)
        ("gom_snapshot", _gom_snapshot_job, ()),  # 🐾 곰발 종목 마감 스냅샷 (15:45 종가 · 18:10 외국인·실적)
        ("sector_map", _ensure_sector_jobs, ()),
        ("futures_snap", _snap_futures, ()),

        ("global_dict", _weekly_global_dictionary, ()),
        ("maintenance", _maintenance_reminders, ()),
        # 🌍 세계 뉴스 07:10·18:10 → JM 뉴스 07·12·18시(브리핑)로 · 🚨 국내·해외 속보 끔 (2026-11-07 JM)
        ("theme_snap", _save_theme_snapshot, ()),
        ("new_highs", _save_new_highs_snapshot, ()),

        ("fut_close", _save_futures_close, ()),
        ("theme_map", _run_theme_map_job, ()),
        ("broker_pc_daily", _run_broker_pc_daily, ()),
        ("broker_pc_track", _run_broker_pc_track, ()),
        ("pc_off_alert", _run_pc_off_alert, ()),

        ("swing_after_close", _run_swing_after_close, ()),
        ("theme_index", _run_theme_index_job, ()),
        ("inst_prefetch", _run_inst_prefetch, ()),
        # ("weekly_review", _run_weekly_review, ()),  # 🛑 금요일 밤 주간 리포트도 끔 (2026-11-07 JM: 주말 리포트 필요 없음)
        ("krx_map", _run_krx_map_job, ()),
        ("new_listing", _run_new_listing_job, ()),
        # ("crab", _run_crab_job, ()),  # 🦀 꽃게 검색기 잠시 끔 (2026-10-30 JM — 검색기 다시 설계 예정, KRX 부담도 줄임)
        ("target_reach", _check_target_reach, ()),
        # 📢 실적 공시 실시간 → 11:00·17:00 공시 총정리로 (2026-11-07)
    ]
    await asyncio.sleep(30)  # 서버 막 켜졌을 때 다른 초기화가 먼저 끝나게 잠깐 대기
    while True:
        for name, fn, args in jobs:
            try:
                await asyncio.to_thread(fn, *args)
            except Exception as e:
                print(f"[daily_jobs] {name} 실패: {e}")
        await asyncio.sleep(300)


_alert_clock_state: dict = {"started": "", "ticks": 0, "last": "", "fns": {}}


@app.get("/api/alert-clock")
def alert_clock_status():
    """⏰ 알림 시계 상태 — 살아있나 · 함수별 마지막 실행/오류 · 오늘 보낸 알림 기록"""
    day = now_kst().strftime("%Y-%m-%d")
    sent = []
    try:
        rows = supabase.table("telegram_sent_log").select("kind").eq("ref_date", day).limit(300).execute().data or []
        sent = sorted({r.get("kind") for r in rows if r.get("kind")})
    except Exception as e:
        sent = [f"(기록 조회 실패: {e})"]
    return {"ok": True, "now": now_kst().strftime("%H:%M:%S"), **_alert_clock_state, "sent_today": sent,
            "pc_alive": _pc_alive(), "nxt_result_age_sec": round(time.time() - (_pc_task_results.get("nxt") or {}).get("got", 0))}


def _alert_clock_loop():
    """⏰ 알림 전용 시계 (2026-11-07 JM) — 15초마다. 무거운 작업 고리(5분)와 따로 돌아서 앞 작업이 길어도 정시에 감.
    각 함수가 '이미 보냈나'를 DB 기록으로 확인하니까 서버가 재시작돼도 두 번 안 감"""
    time.sleep(20)
    _alert_clock_state["started"] = now_kst().strftime("%m-%d %H:%M:%S")
    # 🌅 NXT를 맨 앞에 — 다른 브리핑 만드느라 늦어지지 않게
    fns = (("nxt", _run_nxt_premarket), ("briefs", _run_telegram_briefs), ("us", _run_us_briefs),
           ("health", _ensure_health_report_sent_today), ("jm_report", _run_report_digest))
    while True:
        if now_kst().weekday() >= 5:  # 🛑 주말엔 알림 시계 쉼 (토요일 00:00 미국 개장·일요일 22:25 미국 장전 포함)
            time.sleep(60)
            continue
        _alert_clock_state["ticks"] += 1
        _alert_clock_state["last"] = now_kst().strftime("%H:%M:%S")
        for name, fn in fns:
            t0 = time.time()
            st = _alert_clock_state["fns"].setdefault(name, {})
            try:
                fn()
                st.update({"at": now_kst().strftime("%H:%M:%S"), "sec": round(time.time() - t0, 1)})
            except Exception as e:
                st.update({"at": now_kst().strftime("%H:%M:%S"), "err": f"{type(e).__name__}: {e}"[:300],
                           "err_at": now_kst().strftime("%H:%M:%S")})
                print(f"[alert_clock] {name} 실패: {e}")
        time.sleep(15)


@app.on_event("startup")
async def _start_ws_worker():
    # 🛑 "일하지 마" 스위치 (서버 이전용, 2026-10-29) — 환경변수 JOBS_ENABLED=0 이면 실시간 체결·자동 알림·자동 작업을 전부 끔.
    #    화면용 주소(/api/...)는 그대로 작동. 새 서버 시험할 땐 0, 교대할 때 새 서버 1 + 옛 서버 0
    if os.environ.get("JOBS_ENABLED", "1") == "0":
        print("[startup] JOBS_ENABLED=0 → 실시간 체결·알림·자동 작업 꺼짐 (화면용 주소만 작동)")
        return
    asyncio.create_task(_ws_worker())
    threading.Thread(target=_mq_loop, daemon=True).start()  # ⚡ 매크로 미리 받기
    threading.Thread(target=_daytrade_touch_loop, daemon=True).start()  # 🎯 당일매매 세력평단 터치 (1분마다)
    threading.Thread(target=_alert_clock_loop, daemon=True).start()  # ⏰ 정시 알림 시계 (15초)
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
    todate = _krx_ready_end(today.strftime("%Y%m%d"), 16)

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
            + row.get("연기금", 0)  # HTS·KRX '기관합계'에는 연기금이 포함됨 — 빠져 있어서 기관 숫자가 달랐음 (2026-10-26 수정)
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


def _weekday_end(ymd: str) -> str:
    """주말이면 금요일로 당김 (KRX는 끝 날짜가 토·일이면 빈 값·오류를 줄 때가 있음 — 토요일 0시 넘으면 안 나오던 문제, 2026-10-31)"""
    d = datetime.strptime(ymd, "%Y%m%d")
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def _krx_ready_end(ymd: str, ready_hour: int = 16) -> str:
    """KRX 끝 날짜 정하기 (2026-11-02): 주말이면 금요일, 평일이라도 오늘 데이터가 아직 안 나온 시간이면(장 전·장중)
    전 거래일로. 예전엔 월요일 새벽에 '오늘(월)'을 끝 날짜로 넣어서 KRX가 빈 값을 줬음 (HLB 통합수급 오류)"""
    end = _weekday_end(ymd)
    now = now_kst()
    if end == now.strftime("%Y%m%d") and now.hour < ready_hour:
        d = now - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        end = d.strftime("%Y%m%d")
    return end


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


_sector_vol_cache = {"at": 0.0, "ymd": "", "data": None}


def fetch_sector_volume(limit: int = 30):
    ymd = _latest_trading_day()
    # 🧹 KRX 점검 (2026-10-30): 업종 80여 개를 하나씩 KRX에 물어보는 무거운 작업 → 10분 동안 결과 재사용 + 간격 지키기
    c = _sector_vol_cache
    if c["data"] is not None and c["ymd"] == ymd and time.time() - c["at"] < 600:
        return c["data"][:limit] if isinstance(c["data"], list) else c["data"]

    all_rows = []
    for market in ("KOSPI", "KOSDAQ"):
        tickers = stock.get_index_ticker_list(ymd, market=market)
        for ticker in tickers:
            try:
                name = stock.get_index_ticker_name(ticker)
                ohlcv = _krx_df(stock.get_index_ohlcv_by_date, ymd, ymd, ticker, retries=1)
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
    if rows:
        _sector_vol_cache.update({"at": time.time(), "ymd": ymd, "data": rows})
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


def fetch_institution_type_flow(code: str, days: int = 60, since: str = None):
    today = now_kst()
    fromdate = (today - timedelta(days=days * 2)).strftime("%Y%m%d")  # 주말 감안 여유있게
    if since:  # ⚡ 이어받기: 이미 저장된 날 이후만 (요청이 작아서 빠름)
        fromdate = since
    todate = _krx_ready_end(today.strftime("%Y%m%d"), 16)

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


# ⚡ 기관 유형별을 KRX 대신 한투로 (2026-11-02 JM: KRX가 너무 느려서 응답이 안 오는 것처럼 보임)
#   한투 '종목별 투자자매매동향(일별)' — 미국 서버에서도 빠름. 칸 이름이 다르면 KRX로 자동 대체
_KIS_INST_MAP = {"금융투자": ["scrt"], "보험": ["insu"], "투신": ["ivtr"], "사모": ["pe_fund"], "은행": ["bank"],
                 "기타금융": ["mrbn", "etc_fin"], "연기금": ["fund"], "기타법인": ["etc_corp", "etc_orgt"],
                 "외국인": ["frgn_reg", "frgn"], "기타외국인": ["frgn_nreg"], "개인": ["prsn"]}


def _kis_inst_rows(code: str, end: str):
    j = kis_get("/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily", "FHPTJ04160001",
                {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": end,
                 "FID_ORG_ADJ_PRC": "", "FID_ETC_CLS_CODE": ""})
    return [r for r in (j.get("output2") or j.get("output") or []) if isinstance(r, dict) and r.get("stck_bsop_date")]


def fetch_institution_type_flow_kis(code: str, days: int = 60, since: str = None):
    end = _krx_ready_end(now_kst().strftime("%Y%m%d"), 16)
    raw, cur = [], end
    for _ in range(3):
        rs = _kis_inst_rows(code, cur)
        if not rs:
            break
        raw += rs
        oldest = min(r["stck_bsop_date"] for r in rs)
        if len(raw) >= days or (since and oldest <= since):
            break
        cur = (datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    if not raw:
        raise ValueError("한투 기관 유형별 응답 없음")
    keys = set(raw[0].keys())
    rows, seen = [], set()
    for r in sorted(raw, key=lambda x: x["stck_bsop_date"]):
        d = r["stck_bsop_date"]
        if d in seen or d > end or (since and d < since):
            continue
        seen.add(d)
        for inst, prefixes in _KIS_INST_MAP.items():
            # 한투 칸 이름: 대부분 '_ntby_tr_pbmn', 외국인 등록/비등록만 '_ntby_pbmn' (11/2 확인)
            k = next((f"{p}{suf}" for p in prefixes for suf in ("_ntby_tr_pbmn", "_ntby_pbmn") if f"{p}{suf}" in keys), None)
            if not k:
                continue
            v = float(str(r.get(k) or 0).replace(",", "") or 0)
            rows.append({"stock_code": code, "trade_date": f"{d[:4]}-{d[4:6]}-{d[6:]}", "inst_type": inst, "amount": round(v / 100, 2)})  # 백만원 → 억원
    if not any(x["inst_type"] in ("투신", "연기금") for x in rows):
        raise ValueError(f"한투 칸 이름이 달라요: {sorted(keys)[:30]}")
    return rows[-days * len(_KIS_INST_MAP):]


@app.get("/api/kis-inst-type-debug")
def kis_inst_type_debug(code: str = "005930"):
    """한투 기관 유형별 원본 칸 이름 확인용"""
    try:
        rs = _kis_inst_rows(code, _krx_ready_end(now_kst().strftime("%Y%m%d"), 16))
        return {"ok": True, "count": len(rs), "keys": sorted(rs[0].keys()) if rs else [], "first": rs[0] if rs else None}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-institution-type")
def sync_institution_type_endpoint(code: str):
    """
    통합수급현황 탭에서 종목 검색 시 호출 — 기관 유형별(은행/보험/투신/연기금 등)
    최근 60영업일 순매수를 institution_type_flow 테이블에 저장합니다.
    (외국계 창구별 데이터는 pykrx로 얻을 수 없어 이 엔드포인트에 포함되지 않음)
    """
    # ⚡ KRX 빠르게 (2026-11-02): ① 이미 최신까지 저장돼 있으면 KRX 안 부르고 바로 ② 며칠 밀렸으면 그 뒤만 이어받기
    try:
        want = _krx_ready_end(now_kst().strftime("%Y%m%d"), 16)
        want_d = f"{want[:4]}-{want[4:6]}-{want[6:]}"
        last = supabase.table("institution_type_flow").select("trade_date").eq("stock_code", code).order("trade_date", desc=True).limit(1).execute().data
        last_d = str(last[0]["trade_date"])[:10] if last else None
        if last_d and last_d >= want_d:
            return {"ok": True, "synced": 0, "cached": True}
        since = None
        if last_d and (datetime.strptime(want_d, "%Y-%m-%d") - datetime.strptime(last_d, "%Y-%m-%d")).days <= 20:
            since = (datetime.strptime(last_d, "%Y-%m-%d") - timedelta(days=3)).strftime("%Y%m%d")
        src = "kis"
        try:
            rows = fetch_institution_type_flow_kis(code, since=since)
        except Exception as e:
            print(f"[inst] 한투 실패 → KRX로: {e}")
            src = "krx"
            rows = fetch_institution_type_flow(code, since=since)
        supabase.table("institution_type_flow").upsert(rows).execute()
        return {"ok": True, "synced": len(rows), "incremental": bool(since), "source": src}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _run_inst_prefetch():
    """⚡ 장 마감 뒤(16:20~17:30) 관심종목·장기투자·곰발 종목의 기관 유형별을 미리 받아둠 → 종목분석 열면 바로"""
    now = now_kst()
    if now.weekday() >= 5 or not ((16, 20) <= (now.hour, now.minute) <= (17, 30)):
        return
    day = now.strftime("%Y-%m-%d")
    if _brief_already_sent("job_inst_prefetch", day):
        return
    _mark_brief_sent("job_inst_prefetch", day)
    codes = []
    try:
        codes += [r["code"] for r in supabase.table("jm_watchlist").select("code").limit(200).execute().data or []]
        codes += [r["code"] for r in supabase.table("watchlist").select("code").limit(200).execute().data or []]
    except Exception:
        pass
    d = _crab_pc.get("data") or {}
    for k in CRAB_KEYS:
        codes += [x["code"] for x in d.get(k, []) or []]
    todo = list(dict.fromkeys(codes))[:120]

    def work():  # 뒤에서 (다른 작업·화면을 막지 않게)
        done = 0
        for c in todo:
            try:
                sync_institution_type_endpoint(c)
                done += 1
            except Exception:
                pass
        print(f"[inst_prefetch] {done}종목 기관 유형별 미리 받음")

    threading.Thread(target=work, daemon=True).start()


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
HEALTH_REPORT_HOUR, HEALTH_REPORT_MINUTE = 18, 45  # 🩺 JM 데이터 점검 (2026-11-07)
_health_sent = {"date": None}


# ══════════════════════════════════════════════════════════════════════════
# 🤖 알림 봇 나누기 (2026-10-26) — 봇마다 텔레그램 대화방이 따로 생겨서 알림을 골라 받을 수 있음
#   main  = 기존 봇 (브리핑: 06:15·장전·랭킹·테마·일정·수급·마감·미국장·점검·정비)
#   news  = 📰 뉴스 봇 (세계 주요뉴스 07:10·18:10, 세계 증시 이슈 11:00·15:00·20:20)
#   flash = 🚨 공시·속보 봇 (국내 속보, 잠정실적 공시, IR 예고)
#   report = 🦀 JM 리포트 봇 (브리핑 전부 — 우리 사이트 링크 없이, 리포트에 바로 붙이기용)
#   main  = 기존 봇은 'JM 전용' (목표가 도달·데이터 점검·정비 알림)
#   sports = ⚽ (나중에 스포츠 프로젝트용)
#   Render 환경변수 TELEGRAM_BOT_TOKEN_NEWS / _FLASH / _SPORTS 를 넣으면 그 봇으로, 없으면 기존 봇으로 그대로 감
#   (채팅방이 다르면 TELEGRAM_CHAT_ID_NEWS 등도 넣을 수 있음 — 보통은 같은 사람이라 안 넣어도 됨)
# ══════════════════════════════════════════════════════════════════════════
BRIEF_BOT = {"news_11": "news", "news_15": "news", "news_2020": "news", "ir_16": "flash", "ir_2010": "flash",
             "jmnews_07": "news", "jmnews_12": "news", "jmnews_18": "news", "disc_am": "flash", "disc_pm": "flash"}


def bot(channel: str = "main"):
    if channel == "main":
        return None, None
    tok = os.environ.get(f"TELEGRAM_BOT_TOKEN_{channel.upper()}") or None
    chat = os.environ.get(f"TELEGRAM_CHAT_ID_{channel.upper()}") or None
    return tok, chat


# 🦀 리포트용 알림 꾸미기 (2026-10-27) — 첫 줄은 텔레그램에서 알아보기 쉽게(색 띠 + 🦀 + 【종류】),
#    구분선 아래는 리포트에 그대로 붙일 수 있게. JM 리포트 봇으로 갈 땐 우리 사이트 링크(🔗)를 뺌
BRIEF_STYLE = {
    "closing": ("🟥", "리포트"), "targets": ("🟧", "장전"), "rally": ("🟨", "랭킹"), "flow": ("🟩", "수급"),
    "theme": ("🟦", "테마"), "theme_am": ("🟦", "오전 테마"), "schedule": ("🟪", "일정"),
    "morning": ("🟫", "미국 마감"), "us_pre": ("🟫", "미국 장전"), "us_open30": ("🟫", "미국 개장"),
    "news_11": ("⬜", "세계 이슈"), "news_15": ("⬜", "세계 이슈"), "news_2020": ("⬜", "세계 이슈"),
    "ir_16": ("⬜", "IR 예고"), "ir_2010": ("⬜", "IR 예고"),
    "jmnews_07": ("⬜", "JM 뉴스 시작"), "jmnews_12": ("⬜", "JM 뉴스 중간"), "jmnews_18": ("⬜", "JM 뉴스 마감"),
    "disc_am": ("⬜", "오전 공시"), "disc_pm": ("⬜", "오후 공시"),
}


def format_brief(kind: str, text: str, strip_site_links: bool = False) -> str:
    color, label = BRIEF_STYLE.get(kind, ("⬜", "알림"))
    lines = text.split("\n")
    title = re.sub(r"^[^\w가-힣(]+", "", lines[0]).strip()  # 맨 앞 이모티콘 떼기
    body = lines[1:]
    if strip_site_links:
        site = re.escape(FRONTEND_URL.rstrip("/"))
        cleaned = []
        for ln in body:
            had = site.replace("\\", "") in ln or "🔗" in ln
            ln2 = re.sub(rf"\s*🔗\s*{site}\S*", "", ln)
            ln2 = re.sub(rf"\s*{site}\S*", "", ln2)
            if had and not ln2.strip():
                continue  # 링크만 있던 줄은 통째로 빼기
            cleaned.append(ln2)
        body = cleaned
    head = f"{color}🦀【{label}】 {title}"
    return "\n".join([head, "━━━━━━━━━━━━", f"【{label}】 {title}"] + [ln for ln in body if ln.strip() != "━━━━━━━━━━━━"])


DIGEST_ORDER = ["morning", "targets", "rally", "theme_am", "schedule", "flow", "theme", "closing", "us_pre", "us_open30"]


def _digest_add(kind: str, text: str):
    """오늘 리포트 모음에 쌓기 (리포트용 본문 = 사이트 링크 뺀 것)"""
    if kind not in DIGEST_ORDER:
        return
    try:
        day = now_kst().strftime("%Y-%m-%d")
        key = f"digest:{day}"
        r = supabase.table("brief_cache").select("data").eq("key", key).limit(1).execute().data
        items = (r[0]["data"].get("items") if r else None) or []
        items = [i for i in items if i["kind"] != kind] + [{"kind": kind, "at": now_kst().strftime("%H:%M"),
                                                           "text": format_brief(kind, text, strip_site_links=True)}]
        supabase.table("brief_cache").upsert({"key": key, "data": {"day": day, "items": items}, "updated_at": now_kst().isoformat()}).execute()
    except Exception as e:
        print(f"[digest] 저장 실패: {e}")


def send_brief(kind: str, text: str) -> bool:
    """브리핑 보내기: 뉴스·IR은 각자 봇, 나머지는 JM 리포트 봇(있으면, 사이트 링크 없이) → 없으면 기존 봇"""
    ch = BRIEF_BOT.get(kind)
    if ch:
        return send_telegram(format_brief(kind, text), *bot(ch))
    tok, chat = bot("report")
    ok = send_telegram(format_brief(kind, text, strip_site_links=True), tok, chat) if tok else send_telegram(format_brief(kind, text))
    if ok:
        _digest_add(kind, text)
    # 🚀 09:30 상승 랭킹은 매매용 → JM 전용에도 (사이트 링크 그대로, 바로 눌러서 분봉 확인) (2026-10-28)
    if kind in ("rally",) and tok:
        send_telegram(format_brief(kind, text))
    return ok


def _broker_digest_section() -> str:
    """📝 리포트 PDF용 🏦 창구 요약 — 오늘 장중에 따라간 종목(당일매매)의 외국계 창구·누가 받아줬나·💰 평단 (2026-10-30)"""
    today = now_kst().strftime("%Y-%m-%d")
    codes = [c for c, sm in _bpc.get("samples", {}).items() if sm.get("day") == today]
    if not codes:
        return ""
    names = {}
    try:
        for r in supabase.table("stock_fundamentals").select("code,name").in_("code", codes).execute().data or []:
            names[r["code"]] = r["name"]
    except Exception:
        pass
    q = lambda v: f"{v / 10000:+.1f}만" if abs(v) >= 10000 else f"{v:+,.0f}"
    out = ["【창구】 당일매매 종목 창구 요약 (PC·키움)"]
    for code in codes[:10]:
        try:
            d = broker_pc_get(code)
        except Exception:
            continue
        if not d.get("ok"):
            continue
        nm = names.get(code, code)
        out.append(f"▪ {nm} · 외국계 합계 {q(d.get('foreign_net', 0))}주")
        fr = d.get("foreign", [])[:4]
        if fr:
            out.append("  외국계: " + " · ".join(
                f"{x['name']} {q(x['net'])}" + (f"(평단 {x['avg_buy']:,})" if x.get("avg_buy") else "") for x in fr))
        db = [x for x in d.get("domestic_buy", []) if x["net"] > 0][:3]
        ds = [x for x in d.get("domestic_sell", []) if x["net"] < 0][:3]
        if db:
            out.append("  국내 순매수: " + " · ".join(f"{x['name']} {q(x['net'])}" + (f"(평단 {x['avg_buy']:,})" if x.get("avg_buy") else "") for x in db))
        if ds:
            out.append("  국내 순매도: " + " · ".join(f"{x['name']} {q(x['net'])}" for x in ds))
        fs = [x for x in d.get("foreign", []) if x["net"] < 0]
        if fs and db:
            out.append(f"  누가 받아줬나: {'·'.join(x['name'] for x in fs[:2])} 매도 → {'·'.join(x['name'] for x in db[:2])} 매수")
    return "\n".join(out) if len(out) > 1 else ""


def build_report_digest(day: str = None) -> tuple:
    """🦀 오늘 리포트 모음 — 그날 리포트 봇으로 간 브리핑을 순서대로 한 덩어리로 (본문만, 첫 줄 꾸밈은 빼고)"""
    day = day or now_kst().strftime("%Y-%m-%d")
    r = supabase.table("brief_cache").select("data").eq("key", f"digest:{day}").limit(1).execute().data
    items = (r[0]["data"].get("items") if r else None) or []
    if not items:
        return None, day
    items.sort(key=lambda i: DIGEST_ORDER.index(i["kind"]) if i["kind"] in DIGEST_ORDER else 99)
    parts = [f"🟥🦀【오늘 리포트 모음】 {_kr_date(day)}", "━━━━━━━━━━━━"]
    for it in items:
        body = it["text"].split("\n")[2:]  # 첫 줄(꾸밈)·구분선 빼고 【종류】부터
        parts.append("\n".join(body).strip())
        parts.append("")
    if day == now_kst().strftime("%Y-%m-%d"):
        try:
            bsec = _broker_digest_section()
            if bsec:
                parts.append(bsec)
                parts.append("")
        except Exception as e:
            print(f"[digest] 창구 요약 실패: {e}")
    return "\n".join(parts).strip(), day


def send_document(content: bytes, filename: str, caption: str, token: str = None, chat_id: str = None) -> bool:
    token = token or TELEGRAM_BOT_TOKEN
    chat_id = chat_id or TELEGRAM_CHAT_ID
    try:
        res = requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
                            data={"chat_id": chat_id, "caption": caption[:1000]},
                            files={"document": (filename, content)}, timeout=20)
        if not res.ok:
            print(f"[telegram] 파일 전송 실패: {res.status_code} {res.text[:200]}")
        return res.ok
    except Exception as e:
        print(f"[telegram] 파일 전송 오류: {e}")
        return False


_KR_FONT = {"path": None}


def _kr_font_path():
    """PDF용 한글 글꼴 (나눔고딕) — 서버에 없어서 처음 한 번 내려받아 /tmp에 둠"""
    if _KR_FONT["path"] and os.path.exists(_KR_FONT["path"]):
        return _KR_FONT["path"]
    path = "/tmp/NanumGothic-Regular.ttf"
    if not os.path.exists(path):
        for url in ("https://github.com/google/fonts/raw/main/ofl/nanumgothic/NanumGothic-Regular.ttf",
                    "https://raw.githubusercontent.com/google/fonts/main/ofl/nanumgothic/NanumGothic-Regular.ttf"):
            try:
                r = requests.get(url, timeout=30)
                if r.ok and len(r.content) > 100000:
                    open(path, "wb").write(r.content)
                    break
            except Exception:
                pass
    _KR_FONT["path"] = path if os.path.exists(path) else None
    return _KR_FONT["path"]


def make_report_pdf(text: str, title: str) -> bytes:
    """리포트 모음 → A4 PDF (글자만, 이모티콘은 PDF 글꼴에 없어서 뺌)"""
    import textwrap
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.font_manager import FontProperties
    fp = _kr_font_path()
    font = FontProperties(fname=fp) if fp else FontProperties()
    clean = re.sub(r"[\U0001F000-\U0001FFFF\u2600-\u27BF\uFE0F\u200d\u2B00-\u2BFF]", "", text)
    clean = clean.replace("━", "─").replace("▪", "·").replace("⟨", "(").replace("⟩", ")").replace("▲", "↑").replace("▼", "↓")
    lines = []
    for ln in clean.split("\n"):
        ln = ln.rstrip()
        if not ln:
            lines.append("")
            continue
        indent = len(ln) - len(ln.lstrip())
        lines += textwrap.wrap(ln, width=58, subsequent_indent=" " * (indent + 2)) or [""]
    per = 52
    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        pages = [lines[i:i + per] for i in range(0, max(1, len(lines)), per)]
        for pi, chunk in enumerate(pages, 1):
            fig = plt.figure(figsize=(8.27, 11.69))
            fig.text(0.07, 0.965, title, fontproperties=font, fontsize=13, weight="bold")
            fig.text(0.93, 0.965, f"{pi} / {len(pages)}", fontproperties=font, fontsize=8, ha="right", color="#666")
            y = 0.93
            for ln in chunk:
                bold = ln.startswith("【")
                fig.text(0.07, y, ln, fontproperties=font, fontsize=10.5 if bold else 9.3,
                         weight="bold" if bold else "normal", color="#111" if bold else "#222")
                y -= 0.0172
            pdf.savefig(fig)
            plt.close(fig)
    return buf.getvalue()


def send_report_digest(day: str = None) -> bool:
    """🦀 JM 리포트 봇에만, PDF 파일 한 개로 (2026-10-27 JM 요청: 평일 21:00)"""
    text, day = build_report_digest(day)
    if not text:
        return False
    tok, chat = bot("report")
    title = f"JM 리포트 · {_kr_date(day)}"
    pdf = make_report_pdf(text, title)
    return send_document(pdf, f"JM리포트_{day}.pdf", f"🟥🦀【오늘 리포트 모음】 {_kr_date(day)} — PDF", tok, chat)


def _run_report_digest():
    now = now_kst()
    if now.weekday() >= 5 or not ((21, 0) <= (now.hour, now.minute) <= (21, 40)):
        return
    day = now.strftime("%Y-%m-%d")
    if _brief_already_sent("report_digest", day):
        return
    if send_report_digest(day):
        _mark_brief_sent("report_digest", day)


@app.get("/api/report-digest")
def report_digest_endpoint(day: str = "", send: bool = False, rebuild: bool = False):
    """rebuild=true: 오늘 브리핑을 지금 다시 만들어서 모음을 채움 (테스트용 — 처음 배포한 날처럼 모음이 비어 있을 때)"""
    try:
        if rebuild:
            for k in ("morning", "targets", "rally", "theme_am", "schedule", "flow", "theme", "closing"):
                try:
                    t, _ = BRIEF_BUILDERS[k]()
                    if t:
                        _digest_add(k, t)
                except Exception as e:
                    print(f"[digest] {k} 다시 만들기 실패: {e}")
        text, d = build_report_digest(day or None)
        sent = send_report_digest(d) if (send and text) else False
        return {"ok": True, "day": d, "sent": sent, "text": text}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send_telegram(text: str, token: str = None, chat_id: str = None) -> bool:
    # 🛑 주말(토·일, 한국 시간)엔 알림 전부 OFF (2026-11-07 JM) — '보낸 걸로' 처리해서 같은 알림을 계속 다시 만들지 않게
    if now_kst().weekday() >= 5:
        print("[telegram] 주말 — 알림 끔 (보내지 않음)")
        return True
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


_tg_last_error = {"msg": ""}


def _send_telegram_once(text: str, token: str, chat_id: str) -> bool:
    _tg_last_error["msg"] = ""
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
        if not res.ok:
            print(f"[telegram] 전송 실패: {res.status_code} {res.text[:200]}")
            # 이유를 쉬운 말로 남겨서 테스트 화면에 보여줌 (2026-10-26)
            why = {401: "토큰이 틀렸어요 (Render 환경변수 값 확인)",
                   403: "이 봇 대화방에서 /start를 안 눌렀어요 (봇이 먼저 말을 걸 수 없음)",
                   400: "대화방을 못 찾았어요 (봇 방에서 /start 필요, 또는 채팅 ID 확인)"}.get(res.status_code, "")
            _tg_last_error["msg"] = f"{res.status_code} · {why} · {res.text[:120]}"
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


# 🇺🇸 미국 대형주 한글 이름 (실적발표 일정용) — 목록에 없으면 영문 이름을 깔끔하게 줄여서 씀
US_KR_NAMES = {
    "AAPL": "애플", "MSFT": "마이크로소프트", "NVDA": "엔비디아", "GOOGL": "알파벳(구글)", "GOOG": "알파벳(구글)",
    "AMZN": "아마존", "META": "메타", "TSLA": "테슬라", "AVGO": "브로드컴", "TSM": "TSMC", "BRK.B": "버크셔해서웨이",
    "JPM": "JP모건", "V": "비자", "MA": "마스터카드", "LLY": "일라이릴리", "UNH": "유나이티드헬스", "XOM": "엑슨모빌",
    "JNJ": "존슨앤드존슨", "WMT": "월마트", "PG": "P&G", "HD": "홈디포", "ORCL": "오라클", "COST": "코스트코",
    "ABBV": "애브비", "BAC": "뱅크오브아메리카", "KO": "코카콜라", "PEP": "펩시코", "MRK": "머크", "CVX": "셰브론",
    "NFLX": "넷플릭스", "AMD": "AMD", "ADBE": "어도비", "CRM": "세일즈포스", "TMO": "써모피셔", "ASML": "ASML",
    "SAP": "SAP", "NVO": "노보노디스크", "AZN": "아스트라제네카", "TM": "도요타", "WFC": "웰스파고",
    "MS": "모건스탠리", "GS": "골드만삭스", "C": "씨티그룹", "BLK": "블랙록", "SCHW": "찰스슈왑",
    "AXP": "아메리칸익스프레스", "IBM": "IBM", "INTC": "인텔", "QCOM": "퀄컴", "TXN": "텍사스인스트루먼트",
    "MU": "마이크론", "AMAT": "어플라이드머티리얼즈", "LRCX": "램리서치", "KLAC": "KLA", "CSCO": "시스코",
    "ACN": "액센츄어", "NOW": "서비스나우", "INTU": "인튜이트", "UBER": "우버", "PLTR": "팔란티어",
    "SHOP": "쇼피파이", "DIS": "디즈니", "MCD": "맥도날드", "NKE": "나이키", "SBUX": "스타벅스",
    "CAT": "캐터필러", "GE": "GE에어로스페이스", "BA": "보잉", "RTX": "RTX", "LMT": "록히드마틴",
    "HON": "하니웰", "UNP": "유니언퍼시픽", "UPS": "UPS", "T": "AT&T", "VZ": "버라이즌", "TMUS": "T모바일",
    "CMCSA": "컴캐스트", "PFE": "화이자", "ABT": "애보트", "DHR": "다나허", "AMGN": "암젠",
    "ISRG": "인튜이티브서지컬", "BMY": "BMS", "GILD": "길리어드", "CVS": "CVS헬스", "PGR": "프로그레시브",
    "PLD": "프로로지스", "HDB": "HDFC은행", "BABA": "알리바바", "PDD": "PDD(테무)", "SONY": "소니",
    "HSBC": "HSBC", "RY": "캐나다왕립은행", "TD": "TD뱅크", "SHEL": "셸", "BHP": "BHP", "RIO": "리오틴토",
    "LIN": "린데", "NEE": "넥스트에라에너지", "SPGI": "S&P글로벌", "BX": "블랙스톤", "KKR": "KKR",
    "COP": "코노코필립스", "ARM": "ARM", "ANET": "아리스타네트웍스", "PANW": "팔로알토네트웍스",
    "CRWD": "크라우드스트라이크", "ADP": "ADP", "MDT": "메드트로닉", "SYK": "스트라이커", "VRTX": "버텍스",
    "REGN": "리제네론", "MELI": "메르카도리브레", "BKNG": "부킹홀딩스", "ABNB": "에어비앤비", "DELL": "델",
    "MRVL": "마벨", "APP": "앱러빈", "SNOW": "스노우플레이크", "COIN": "코인베이스", "UL": "유니레버",
    "DEO": "디아지오", "NVS": "노바티스", "SAN": "산탄데르", "MUFG": "미쓰비시UFJ", "SMFG": "미쓰이스미토모",
    "IBN": "ICICI은행", "INFY": "인포시스", "TTE": "토탈에너지스", "BP": "BP", "HMC": "혼다", "ETN": "이튼",
    "DE": "디어앤컴퍼니", "MMM": "3M", "TJX": "TJX", "LOW": "로우스", "ELV": "엘레반스헬스", "CI": "시그나",
    "MO": "알트리아", "PM": "필립모리스", "USB": "US뱅코프", "PNC": "PNC", "COF": "캐피털원", "MMC": "마시앤드매클레넌",
    "CB": "처브", "ICE": "ICE", "CME": "CME그룹", "GEV": "GE버노바", "VRT": "버티브", "TT": "트레인",
    "WM": "웨이스트매니지먼트", "AMT": "아메리칸타워", "EQIX": "에퀴닉스", "TSCO": "트랙터서플라이",
    "LULU": "룰루레몬", "ADI": "아날로그디바이시스", "SNPS": "시놉시스", "CDNS": "케이던스", "FI": "파이서브",
    "WDAY": "워크데이", "TEAM": "아틀라시안", "DDOG": "데이터독", "ZS": "지스케일러", "SPOT": "스포티파이",
    "NTES": "넷이즈", "JD": "징둥닷컴", "BIDU": "바이두", "TCEHY": "텐센트", "RACE": "페라리", "STLA": "스텔란티스",
    "GM": "GM", "F": "포드", "LVS": "라스베이거스샌즈", "MAR": "메리어트", "HLT": "힐튼", "DAL": "델타항공",
    "UAL": "유나이티드항공", "FDX": "페덱스", "NOC": "노스롭그루먼", "GD": "제너럴다이내믹스",
}
_NAME_SUFFIX_RE = re.compile(
    r"(,?\s*(Inc\.?|Incorporated|Corporation|Corp\.?|Company|Co\.?|Ltd\.?|Limited|plc|PLC|N\.V\.|S\.A\.|AG|SE|Holdings?|Group|& Co))+\s*$"
)


def _us_name(symbol: str, name: str) -> str:
    if symbol in US_KR_NAMES:
        return US_KR_NAMES[symbol]
    n = re.sub(r"\(The\)", "", name or symbol).strip()
    n = re.sub(r"^The\s+", "", n)
    for _ in range(3):
        n = _NAME_SUFFIX_RE.sub("", n).strip(" ,")
    return n or symbol


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
                        "title": f"美) {_us_name(r.get('symbol') or '', r.get('name') or '')} 실적발표" + (f" ({tlabel})" if tlabel else ""),
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


# ══════════════════════════════════════════════════════════════════════════
# 🆕 신규상장 (IPO) — 38커뮤니케이션 "신규상장"(상장일·공모가) + "수요예측 결과"(기관 경쟁률·의무보유확약)
# 표 모양이 바뀌어도 머리글(신규상장일·공모가·기관경쟁률·의무보유확약)을 찾아 읽도록 만듦
# ══════════════════════════════════════════════════════════════════════════
_ipo_cache = {"at": 0.0, "data": None}


def _38_table(params: dict, must: list):
    res = requests.get("http://www.38.co.kr/html/fund/index.htm", params=params,
                       headers={"User-Agent": "Mozilla/5.0", "Referer": "http://www.38.co.kr/"}, timeout=12)
    soup = BeautifulSoup(res.content.decode("euc-kr", errors="replace"), "html.parser")
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for hi, tr in enumerate(rows):
            head = [c.get_text(" ", strip=True).replace(" ", "") for c in tr.find_all(["td", "th"])]
            if all(any(m in h for h in head) for m in must):
                out = []
                for tr2 in rows[hi + 1:]:
                    cells = [c.get_text(" ", strip=True) for c in tr2.find_all("td")]
                    if len(cells) >= len(head) - 1 and cells and cells[0]:
                        out.append(dict(zip(head, cells)))
                if out:
                    return head, out
    return None, []


def _num(v):
    try:
        return float(re.sub(r"[^\d.]", "", str(v)) or "nan")
    except ValueError:
        return None


def _ipo_name(v: str) -> str:
    return re.sub(r"\((유가|코스닥|코넥스)\)|\s+", "", v or "").strip()


def fetch_kr_ipo_listings() -> list:
    """신규상장 목록 [{name, date, offer, comp, lockup}] — 30분 보관"""
    c = _ipo_cache
    if c["data"] is not None and time.time() - c["at"] < 1800:
        return c["data"]
    out = {}
    try:
        head, rows = _38_table({"o": "nw"}, ["신규상장일", "공모가"])
        for r in rows:
            name = _ipo_name(next((v for k, v in r.items() if "기업명" in k or "종목명" in k), list(r.values())[0]))
            d = next((v for k, v in r.items() if "신규상장일" in k), "")
            m = re.search(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})", d)
            if not name or not m:
                continue
            offer = next((_num(v) for k, v in r.items() if k.startswith("공모가") and "대비" not in k), None)
            out[name] = {"name": name, "date": f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", "offer": offer, "comp": None, "lockup": None}
    except Exception as e:
        print(f"[ipo] 신규상장 실패: {e}")
    try:
        head, rows = _38_table({"o": "r1"}, ["기관경쟁률"])
        for r in rows:
            name = _ipo_name(next((v for k, v in r.items() if "기업명" in k or "종목명" in k), list(r.values())[0]))
            if name in out:
                out[name]["comp"] = next((v for k, v in r.items() if "기관경쟁률" in k), None)
                out[name]["lockup"] = next((v for k, v in r.items() if "의무보유" in k), None)
                if not out[name]["offer"]:
                    out[name]["offer"] = next((_num(v) for k, v in r.items() if k.startswith("공모가") and "희망" not in k), None)
    except Exception as e:
        print(f"[ipo] 수요예측 실패: {e}")
    data = sorted(out.values(), key=lambda x: x["date"])
    c.update({"at": time.time(), "data": data})
    return data


def _ipo_read(x: dict) -> str:
    """🆕 공모 지표 한 줄 해석 (2026-11-02 JM) — 기관경쟁률 = 관심, 의무보유확약 = 첫날 나올 물량"""
    def num(v):
        m = re.search(r"[\d,.]+", str(v or ""))
        try:
            return float(m.group(0).replace(",", "")) if m else None
        except ValueError:
            return None
    comp, lock = num(x.get("comp")), num(x.get("lockup"))
    if comp is None and lock is None:
        return ""
    c_txt = (None if comp is None else "🔴 관심 아주 높음" if comp >= 1000 else "🔴 관심 높음" if comp >= 500
             else "⚪ 관심 보통" if comp >= 100 else "🔵 관심 낮음")
    l_txt = (None if lock is None else "🔴 첫날 물량 적음" if lock >= 30 else "⚪ 첫날 물량 보통" if lock >= 10
             else "🔵 첫날 물량 많음" if lock >= 5 else "🔵 첫날 물량 아주 많음")
    hot, tight = (comp or 0) >= 500, (lock or 0) >= 15
    if comp is not None and lock is not None:
        if hot and tight:
            sumtxt = "→ 관심↑·물량↓: 첫날 강세 기대"
        elif hot and not tight:
            sumtxt = "→ 관심↑·물량↑: 시초 뒤 기관 매도로 변동성 큼, 3분봉 눌림 확인"
        elif not hot and tight:
            sumtxt = "→ 관심 보통·물량↓: 흐름 보며"
        else:
            sumtxt = "→ 관심↓·물량↑: 약세 주의"
    else:
        sumtxt = ""
    return "   " + " · ".join(t for t in (c_txt, l_txt) if t) + (f" {sumtxt}" if sumtxt else "")


def _ipo_line(x: dict) -> str:
    bits = [f"공모가 {x['offer']:,.0f}원" if x.get("offer") else None,
            f"기관경쟁률 {x['comp']}" if x.get("comp") else None,
            f"의무보유확약 {x['lockup']}" if x.get("lockup") else None]
    line = f"· {x['name']} — " + " · ".join(b for b in bits if b) if any(bits) else f"· {x['name']}"
    rd = _ipo_read(x)
    return line + (f"\n{rd}" if rd else "")


def _fetch_kr_listing(start: str, end: str) -> list:
    return [{"date": x["date"], "time": None, "title": f"신규상장 : {x['name']}", "major": True, "note": None, "kind": "kr_listing"}
            for x in fetch_kr_ipo_listings() if start <= x["date"] <= end]


@app.get("/api/ipo")
def ipo_endpoint():
    """신규상장 미리보기 — 표를 제대로 읽는지 확인용"""
    try:
        return {"ok": True, "items": fetch_kr_ipo_listings()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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


# 🌏 해외 중앙은행 (⚠️ 매년 1월 갱신 — 정비 알림에 포함). 결과 발표는 한국 시각 기준
BOJ_DECISION_DATES = ["2026-01-23", "2026-03-19", "2026-04-28", "2026-06-16", "2026-07-31", "2026-09-18", "2026-10-30", "2026-12-18"]
# 📅 MSCI 분기 리뷰 (MSCI 공식 발표, 2026-10-30 확인) — 발표는 런던 시간이라 한국은 다음 날 아침에 확인
#   (발표일, 적용 기준 장 마감일 = 효력일 전 거래일). 다음 8개 일정은 매 리뷰 때 MSCI가 새로 알려줌 → 1월 2일 정비 때 갱신
MSCI_REVIEW_DATES = [("2026-11-11", "2026-11-30"), ("2027-02-09", "2027-02-26")]
ECB_DECISION_DATES = ["2026-02-05", "2026-03-19", "2026-04-30", "2026-06-11", "2026-07-23", "2026-09-10", "2026-10-29", "2026-12-17"]
# 🇰🇷 날짜가 매번 다른 국내 지표 (한국은행 공표일정 기준, ⚠️ 매년 갱신)
KR_GDP_DATES = {"2026-10-27": "3분기 실질 GDP 속보", "2027-01-26": "4분기 실질 GDP 속보"}


def _last_weekday(y: int, m: int) -> datetime:
    d = (datetime(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1))
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _nth_weekday(y: int, m: int, n: int) -> datetime:
    d, k = datetime(y, m, 1), 0
    while True:
        if d.weekday() < 5:
            k += 1
            if k == n:
                return d
        d += timedelta(days=1)


def _fetch_global_cb(start: str, end: str) -> list:
    out = []
    for d in BOJ_DECISION_DATES:
        if start <= d <= end:
            out.append({"date": d, "time": "점심 무렵", "title": "日) 일본은행(BOJ) 금리 결정", "major": True,
                        "note": "엔화·일본 증시에 영향 (엔캐리 청산 이슈 주의)", "kind": "us_macro"})
    for d in ECB_DECISION_DATES:
        if start <= d <= end:
            summer = 3 < int(d[5:7]) < 10 or (d[5:7] == "10" and int(d[8:]) < 25) or (d[5:7] == "03" and int(d[8:]) > 28)
            out.append({"date": d, "time": "21:15" if summer else "22:15", "title": "유로존) 유럽중앙은행(ECB) 금리 결정", "major": True,
                        "note": "유로화·달러·유럽 증시에 영향", "kind": "us_macro"})
    for ann, eff in MSCI_REVIEW_DATES:
        nxt = (datetime.strptime(ann, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        if start <= nxt <= end:
            out.append({"date": nxt, "time": "아침", "title": "MSCI 분기 리뷰 발표 (편입·편출 종목)", "major": True,
                        "note": "외국인 패시브 자금 — 편입 종목 미리 움직임 · 골드만·모건스탠리 창구 주목", "kind": "us_macro"})
        if start <= eff <= end:
            out.append({"date": eff, "time": "장 마감 동시호가", "title": "MSCI 리밸런싱 적용일", "major": True,
                        "note": "종가에 외국인 대량 매매 → 편입·편출 종목 종가 급변 주의", "kind": "us_macro"})
    return out


def _fetch_kr_schedule(start: str, end: str) -> list:
    out = []
    for d, t in KR_GDP_DATES.items():
        if start <= d <= end:
            out.append({"date": d, "time": "08:00", "title": f"韓) {t} (한국은행)", "major": True,
                        "note": "한국 경제 성장률 — 내수·경기민감주에 영향", "kind": "kr_macro"})
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
        # 통계청 정기 지표 (발표 규칙 기준 예정일 — 실제 날짜는 하루 이틀 다를 수 있음)
        for d_obj, title, note in ((_nth_weekday(y, m, 2), "韓) 소비자물가동향 (통계청·예정)", "국내 물가 — 금리·내수주에 영향"),
                                   (_nth_weekday(y, m, 8), "韓) 고용동향 (통계청·예정)", "고용 흐름 — 내수 경기 확인"),
                                   (_last_weekday(y, m), "韓) 산업활동동향 (통계청·예정)", "생산·소비·투자 — 경기 흐름 확인")):
            ds = d_obj.strftime("%Y-%m-%d")
            if start <= ds <= end:
                out.append({"date": ds, "time": "08:00", "title": title, "major": False, "note": note, "kind": "kr_macro"})
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


_kr_ir_cache = {"at": 0.0, "data": []}


def _dart_plain_text(rcept_no: str) -> str:
    res = requests.get("https://opendart.fss.or.kr/api/document.xml", params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}, timeout=15)
    if res.content[:2] != b"PK":
        return ""
    with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
        raw = b"".join(zf.read(n) for n in zf.namelist())
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("euc-kr", errors="replace")
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return re.sub(r"\s+", " ", BeautifulSoup(text, "html.parser").get_text(" "))


def _fetch_kr_ir(start: str, end: str) -> list:
    """최근 14일 DART 'IR 개최·결산실적 공시예고' 원문에서 날짜를 꺼내 → 날짜별 '실적발표·IR : A·B·C' (3시간 보관)"""
    if not DART_API_KEY:
        return []
    c = _kr_ir_cache
    if not c["data"] or time.time() - c["at"] > 3 * 3600:
        now = now_kst()
        rows = _dart_list((now - timedelta(days=14)).strftime("%Y%m%d"), now.strftime("%Y%m%d"), max_pages=6)
        items, seen = [], set()
        for r in rows:
            nm = r.get("report_nm") or ""
            if not r.get("stock_code") or "정정" in nm or not re.search(r"기업설명회|IR\)?\s*개최|결산실적공시\s*예고", nm):
                continue
            if r["corp_name"] in seen:
                continue
            try:
                txt = _dart_plain_text(r["rcept_no"])
            except Exception:
                continue
            m = re.search(r"(?:일시|일자|예정일)[^0-9]{0,15}(20\d{2})\s*[.\-년]\s*(\d{1,2})\s*[.\-월]\s*(\d{1,2})", txt)
            if not m:
                continue
            seen.add(r["corp_name"])
            items.append({"date": f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", "name": r["corp_name"], "code": r["stock_code"],
                          "kind": "실적" if "결산" in nm else "IR"})
            time.sleep(0.15)
        c.update({"at": time.time(), "data": items})
    by_day = {}
    for x in c["data"]:
        if start <= x["date"] <= end:
            by_day.setdefault(x["date"], []).append(x)
    out = []
    for d, xs in sorted(by_day.items()):
        names = [x["name"] for x in xs]
        tags = []
        try:
            dd = supabase.table("stock_dictionary").select("tags").in_("code", [x["code"] for x in xs[:6]]).execute().data or []
            for r in dd:
                tags += (r.get("tags") or [])[:1]
        except Exception:
            pass
        note = ("업종: " + "·".join(dict.fromkeys(tags))) if tags else "해당 종목·업종에 영향"
        out.append({"date": d, "time": None, "title": "韓) 실적발표·IR : " + "·".join(names[:5]) + (f" 외 {len(names) - 5}" if len(names) > 5 else ""),
                    "major": len(names) >= 3, "note": note, "kind": "kr_macro"})
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
        ("신규상장", _fetch_kr_listing),
        ("해외 중앙은행", _fetch_global_cb),
        ("국내 실적발표·IR", _fetch_kr_ir),
    ):
        try:
            events += fn(start, end)
        except Exception as e:
            errors.append(f"{label}: {e}")
            print(f"[calendar] {label} 실패: {e}")
    events.sort(key=lambda e: (e["date"], e["time"] or "99:99", not e["major"]))
    uniq, seen = [], set()  # 같은 날·같은 제목은 하나만 (예: 알파벳 실적발표가 두 줄 나오던 문제)
    for e in events:
        k = (e["date"], re.sub(r"\s+", "", e["title"]))
        if k not in seen:
            seen.add(k)
            uniq.append(e)
    events = uniq
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
# ⏰ 2026-11-07 JM 알림 개편 — 정해진 시각에 '딱' (15초 시계 + 미리 만들어 두기) · 중복 없음
#    없앤 것: 세계 증시 이슈(11·15·20:20) · 실적발표 예고/IR(16·20:10 → 공시 총정리에 합침) · 마감 리포트 초안(20:40)
BRIEF_SCHEDULE = {
    "morning": (6, 15),    # 🇺🇸 미국 마감
    "jmnews_07": (7, 0),   # 📰 JM 뉴스 — 시작
    "targets": (7, 50),    # 📋 장전 리포트
    "rally": (9, 30),      # 🚀 아침 상승 랭킹
    "theme_am": (10, 0),   # 🌅 오전장 테마 이슈
    "disc_am": (11, 0),    # 📢 오전 공시 총정리
    "jmnews_12": (12, 0),  # 📰 JM 뉴스 — 중간
    "schedule": (14, 0),   # 🗓 주요 일정
    "flow": (16, 0),       # 💰 오늘의 수급
    "disc_pm": (17, 0),    # 📢 오후 공시 총정리
    "jmnews_18": (18, 0),  # 📰 JM 뉴스 — 마감
    "theme": (20, 0),      # 🔥 오늘의 테마 이슈
}
# 만드는 데 오래 걸리는 브리핑은 이만큼(분) 먼저 만들어 두고 정각에 보냄
BRIEF_LEAD = {"morning": 3, "jmnews_07": 3, "targets": 4, "rally": 5, "theme_am": 5, "disc_am": 3, "jmnews_12": 3,
              "schedule": 6, "flow": 4, "disc_pm": 3, "jmnews_18": 3, "theme": 5}
_prebuilt: dict = {}   # (kind, day) → (text, ref) / "building"


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
        arrow = "🔴▲" if q["pct"] > 0 else "🔵▼" if q["pct"] < 0 else "⚪"  # 한국식: 상승 빨강·하락 파랑
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
    up = sum(1 for r in idx[:3] if r.startswith("🔴"))
    mood = "3대 지수 모두 상승" if up == 3 else "3대 지수 모두 하락" if up == 0 else "3대 지수 혼조"
    eu_rows = []
    for label, sym, unit in EUROPE_SYMBOLS:
        try:
            q = _yahoo_quote(sym)
        except Exception:
            q = None
        if not q:
            continue
        arrow = "🔴▲" if q["pct"] > 0 else "🔵▼" if q["pct"] < 0 else "⚪"  # 한국식: 상승 빨강·하락 파랑
        val = f"{q['price']:,.2f}" + ("p" if unit == "p" else "") if label != "유로/달러" else f"{q['price']:.4f}"
        eu_rows.append(f"{arrow} {label} {val} ({q['pct']:+.2f}%)")
    text = (
        f"☀️ 미국 증시 모닝 브리핑 ({us_date or '-'} 미국장 마감)\n━━━━━━━━━━━━\n"
        f"📌 {mood}\n\n" + "\n".join(rows[:5])
        + ("\n\n🇪🇺 유럽 마감\n" + "\n".join(eu_rows) if eu_rows else "")
        + "\n\n" + "\n".join(rows[5:])
    )
    # 확장 부분은 시간 제한(45초)을 두고, 실패해도 기본 마감 알림은 무조건 발송 (2026-10-23: 06:15 알림 누락 수정)
    import concurrent.futures as _cf
    with _cf.ThreadPoolExecutor(max_workers=1) as ex:
        try:
            extra = ex.submit(_us_close_extras).result(timeout=45)
            if extra:
                text += "\n\n" + extra
        except Exception as e:
            print(f"[brief:morning] 확장 실패(기본 알림은 발송): {e}")
    # 🌙 내가 잠든 사이 해외 속보 (밤에 모아둔 것)
    try:
        allnight = [i for i in _flash_state.get("pending", []) if i.get("cat") == "gl"]
        night = allnight[-8:]  # 가장 최근 8개
        if night:
            label = "🌙 주말·밤사이 해외 속보" if now_kst().weekday() == 0 else "🌙 밤사이 해외 속보"
            text += f"\n\n{label}\n" + "\n\n".join(i["text"] for i in night) + (f"\n(외 {len(allnight) - len(night)}건)" if len(allnight) > len(night) else "")
            _flash_state["morning_keys"] = [i["key"] for i in allnight]
    except Exception as e:
        print(f"[brief:morning] 밤사이 속보 실패: {e}")
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
        parts.append("🔴▲ 목표가 상향 TOP\n" + "\n".join(line(r, pct) for pct, r in ups[:10]))
    if downs:
        downs.sort(key=lambda x: x[0])
        parts.append("🔵▼ 목표가 하향\n" + "\n".join(line(r, pct) for pct, r in downs[:5]))
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
    lines = [f"💰 오늘의 수급 ({_kr_date(day)}) · KRX 기준(NXT 제외)\n━━━━━━━━━━━━"]
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
    fut = None
    try:
        fut = fetch_futures_investor()
        summary.append(f"선물 순{'매수' if fut['foreign_qty'] > 0 else '매도'}")
    except Exception as e:
        print(f"[brief:flow] 선물 실패: {e}")
    if summary:
        lines.insert(1, "외국인 · " + " / ".join(summary) + "\n")
    # 📌 시장 체크 (재진입 조건 3가지) — 외국인 현물 순매도 1조 이하 · 외국인 선물 순매수 · 코스피 120일선 종가 지지
    try:
        chk = []
        r = supabase.table("market_flow_trend").select("foreign_amt").eq("market", "KOSPI").eq("trade_date", day).limit(1).execute().data
        if r:
            fa = float(r[0]["foreign_amt"] or 0)
            chk.append(f"{'✅' if fa > -10000 else '❌'} 외국인 현물 순매도 1조 이하 — {_fmt_eok(fa)}")
        if fut:
            chk.append(f"{'✅' if fut['foreign_qty'] > 0 else '❌'} 외국인 선물 순매수 — {fut['foreign_qty']:+,.0f}계약")
        try:
            res = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/%5EKS11", params={"range": "1y", "interval": "1d"},
                               headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
            closes = [c for c in res["chart"]["result"][0]["indicators"]["quote"][0]["close"] if c]
            if len(closes) >= 120:
                ma120 = sum(closes[-120:]) / 120
                chk.append(f"{'✅' if closes[-1] >= ma120 else '❌'} 코스피 120일선 종가 지지 — {closes[-1]:,.2f} / 120일선 {ma120:,.2f}")
        except Exception:
            pass
        if chk:
            ok = sum(1 for c in chk if c.startswith("✅"))
            lines.append(f"\n📌 시장 체크 — {ok}/{len(chk)} 충족 (재진입 조건)\n" + "\n".join(chk))
    except Exception as e:
        print(f"[brief:flow] 시장 체크 실패: {e}")
    try:  # 🔥 52주 신고가 (근접 1% 이내 · 거래대금 50억↑ · 상승)
        hs = [h for h in _new_highs_for_brief() if h["value_eok"] >= 50 and h["pct"] > 0][:5]
        if hs:
            lines.append("\n🔥 52주 신고가 (근접 1% 이내 · 거래대금 50억↑)\n" + "\n".join(
                f"{i} {h['name']} {h['pct']:+.2f}% · {_fmt_eok(h['value_eok'], signed=False)}" for i, h in enumerate(hs, 1)))
    except Exception as e:
        print(f"[brief:flow] 52주 신고가 실패: {e}")
    # 사모 TOP5 — 뺌 (2026-11-07 JM)
    try:
        if True:
            raise StopIteration
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
    except StopIteration:
        pass
    except Exception as e:
        print(f"[brief:flow] 사모 TOP5 실패: {e}")
    try:  # 🌏 아시아 마감 한 줄 (일본·중국·홍콩)
        al = []
        for sym, nm in (("^N225", "니케이"), ("000001.SS", "상해"), ("^HSI", "항셍"), ("^TWII", "대만")):
            q = _yahoo_quote(sym)
            if q:
                al.append(f"{nm} {'🔴▲' if q['pct'] > 0 else '🔵▼'}{q['pct']:+.2f}%")
        if al:
            lines.append("\n🌏 아시아: " + " · ".join(al))
    except Exception as e:
        print(f"[brief:flow] 아시아 실패: {e}")
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


THEME_SKIP_NAMES = re.compile(r"신규상장|하반기|상반기|스팩|SPAC|우선주|관리종목|정리매매")


def _theme_news(names: list, limit: int = 2) -> list:
    """테마 종목들의 최근 하루 뉴스에서 쓸 만한 제목 몇 개 (기획·리포트·분쟁 기사 제외)"""
    out, seen = [], set()
    for nm in names:
        try:
            res = requests.get("https://news.google.com/rss/search", params={"q": f'"{nm}" when:1d', "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                               headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
            for it in ET.fromstring(res.content).iter("item"):
                title = (it.findtext("title") or "").strip()
                src = (it.findtext("source") or "").strip()
                if src and title.endswith(f" - {src}"):
                    title = title[: -len(src) - 3]
                if nm not in title or FLASH_KR_SKIP.search(title) or FLASH_KR_TAG.search(title) or FLASH_KR_SKIP2.search(title) or FLASH_KR_SKIP_SRC.search(src):
                    continue
                k = re.sub(r"[^가-힣A-Za-z0-9]", "", title)[:30]
                if k in seen:
                    continue
                seen.add(k)
                out.append({"name": nm, "title": title, "src": src, "link": (it.findtext("link") or "").strip()})
                break
        except Exception:
            pass
        if len(out) >= limit:
            break
        time.sleep(0.15)
    return out


def build_theme_brief(top_n: int = 5, title: str = "🔥 오늘의 테마 이슈"):
    """오늘 '진짜 돈이 몰린' 테마 — 2026-10-23 개선
    · 점수 = 오른 종목들의 거래대금 합 (+ 주도주 등락률). 평균 등락률 순이던 예전 방식은 조용한 소형 테마가 위로 올라왔음
    · 주도주 거래대금 100억↑·+3%↑ 테마만, 같은 주도주는 한 테마에만(디티에스가 3개 테마를 차지하던 문제)
    · 거래대금 10억 미만 종목은 숨김, 테마마다 ↳ 왜 올랐나(뉴스) + 📰 관련 뉴스 링크"""
    themes = fetch_naver_theme_list(limit=40)
    now = now_kst()
    scored = []
    naver_order = True  # 🦀 2026-11-07 JM: 우리 시장분석(네이버 테마 순위) TOP 5 그대로 — 돈 점수로 다시 줄 세우지 않음
    for t in themes:
        if THEME_SKIP_NAMES.search(t["theme_name"]):
            continue
        if naver_order and len(scored) >= top_n:
            break
        try:
            stocks, _ = fetch_naver_theme_detail(t["theme_no"], limit=30)
        except Exception:
            continue
        for x in stocks:
            x["eok"] = _to_eok_from_naver(x["trading_value"], x.get("price", 0), x.get("volume", 0))
        risers = [x for x in stocks if x["change_pct"] > 0 and x["eok"] >= 10]
        if not risers:
            continue
        leader = max(risers, key=lambda x: (x["eok"] >= 100, x["change_pct"] * min(x["eok"], 3000) ** 0.5))
        if not naver_order and (leader["eok"] < 100 or leader["change_pct"] < 3):
            continue
        money = sum(x["eok"] for x in risers)
        scored.append({"theme": t, "leader": leader, "stocks": sorted(risers, key=lambda x: -x["eok"])[:4],
                       "score": money ** 0.5 * (1 + leader["change_pct"] / 10), "money": money})
        time.sleep(0.05)
    if not naver_order:
        scored.sort(key=lambda x: -x["score"])
    picked, used = [], set()
    for sc in scored:
        if sc["leader"]["stock_code"] in used and not naver_order:
            continue
        used.add(sc["leader"]["stock_code"])
        picked.append(sc)
        if len(picked) >= top_n:
            break
    parts = [f"{title} {now.month}/{now.day}({WEEKDAY_KR[now.weekday()]} {now.strftime('%H:%M')})\n━━━━━━━━━━━━"]
    if not picked:
        parts.append("오늘은 돈이 크게 몰린 테마가 없어요 (주도주 거래대금 100억·+3% 이상 기준).")
    for sc in picked:
        t, lead = sc["theme"], sc["leader"]
        rank = picked.index(sc) + 1
        tp = t.get("change_pct")
        lines = [f"📌 {rank}. {t['theme_name']}" + (f" {tp:+.2f}%" if isinstance(tp, (int, float)) else "")
                 + f" · 오른 종목 거래대금 {_fmt_eok(sc['money'], signed=False)}"]
        for x in sc["stocks"]:
            tags = (" 🦀" if x is lead else "") + (" 🔥" if x["change_pct"] >= 29 else "") + _gom_mark(x["stock_code"])
            lines.append(f" {x['stock_name']} {x['change_pct']:+.2f}% · {_fmt_eok(x['eok'], signed=False)}{tags}")
        news = _theme_news([x["stock_name"] for x in sc["stocks"]], limit=2)
        if news:
            lines.append(f"   ↳ {news[0]['title']} ({news[0]['src']})")
            lines.append("📰 관련 뉴스: " + " · ".join(f"{n['name']} 📎 {n['link']}" for n in news))
        else:
            lines.append("   ↳ 관련 뉴스 없음 — 수급·테마 순환 가능성")
        parts.append("\n".join(lines))
    parts.append("※ 우리 시장분석(네이버 테마 순위) TOP 5 · 🦀 = 테마 1등 · ●곰발 · ○곰발 통합 · 🎯 둘 다 · 거래대금 10억 미만 종목 제외 · ↳는 관련 뉴스(원인이 아닐 수도)")
    return "\n\n".join(parts), now.strftime("%Y-%m-%d")


def _event_line(e: dict) -> str:
    star = " ⚡" if e.get("major") else ""
    t = f" ({e['time']})" if e.get("time") else ""
    line = f"· {e['title']}{star}{t}"
    if e.get("note") and e.get("kind") in ("us_macro", "kr_macro"):
        line += f"\n  ↳ {e['note']}"
    elif e.get("kind") == "us_earnings":
        sym = (e.get("note") or "").split(" · ")[0]
        if sym in KR_LINKS:
            line += f"\n  ↳ 🇰🇷 관련: {KR_LINKS[sym]}"
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
        plans = _dedup_by_corp([r for r in rows if _classify_disclosure(r.get("report_nm")) == "plan"])[:10]
        if plans:
            parts.append("📢 실적발표 예고·IR 공시 (최근 7일)\n" + "\n".join(_disclosure_line(r, set()) for r in plans))
    except Exception as e:
        print(f"[brief:schedule] 실적 예고 실패: {e}")
    if cal.get("errors"):
        parts.append("※ 일부 일정 출처를 못 불러왔어요: " + " / ".join(e[:120] for e in cal["errors"]))
    return "\n\n".join(parts), today


def _stock_link(code: str) -> str:
    return f"{FRONTEND_URL}/stock-analysis?code={code}"


def _theme_top_now(n: int = 3):
    themes = sorted(fetch_naver_theme_list(limit=25), key=lambda t: t["change_pct"], reverse=True)[:n]
    out = []
    for t in themes:
        try:
            stocks, _ = fetch_naver_theme_detail(t["theme_no"], limit=20)
        except Exception:
            stocks = []
        top = sorted(stocks, key=lambda x: x["change_pct"], reverse=True)[:2]
        out.append({"name": t["theme_name"], "pct": t["change_pct"],
                    "stocks": [{"name": x["stock_name"], "pct": x["change_pct"], "code": x["stock_code"]} for x in top]})
    return out


def _save_theme_snapshot():
    """평일 15:50 이후 하루 한 번 — 오늘 강세 테마를 저장해 다음 날 장전 리포트에 씀"""
    now = now_kst()
    if now.weekday() >= 5 or (now.hour, now.minute) < (15, 50):
        return
    key = now.strftime("%Y-%m-%d")
    if _brief_already_sent("job_theme_snap", key):
        return
    try:
        if _latest_trading_day() != now.strftime("%Y%m%d"):
            return
        data = {"date": key, "themes": _theme_top_now()}
        if data["themes"] and any(t["pct"] for t in data["themes"]):
            supabase.table("brief_cache").upsert({"key": "theme_top_prevday", "data": data, "updated_at": now.isoformat()}).execute()
            _mark_brief_sent("job_theme_snap", key)
    except Exception as e:
        print(f"[theme_snap] 실패: {e}")


MORNING_NEWS_QUERIES = ['"수주"', '"공급계약"', '"단독"', '"정부" 지원', '"정책" 수혜', '"FDA"', '"임상"', '"최대 실적"', '"신고가"', '"특징주"']


def _morning_news_picks(n: int = 3) -> list:
    """밤사이~아침(최근 14시간) 국내 기사 중 상장사 이름이 들어간 굵직한 것 2~3개
    → 헤드라인 + 원문 링크 + 🏷 테마(내 종목 사전) + 관련주(같은 테마의 다른 종목)"""
    from email.utils import parsedate_to_datetime
    names = _flash_stock_names()
    now = now_kst()
    weight = [(r"단독", 3), (r"수주|공급계약|계약\s*체결", 3), (r"FDA|품목허가|임상.{0,6}(성공|승인)", 3), (r"최대\s*실적|흑자\s*전환", 2),
              (r"정부|정책|지원|규제\s*완화", 2), (r"특징주|신고가|급등", 1)]
    cands, seen_codes = [], set()
    for q in MORNING_NEWS_QUERIES:
        try:
            res = requests.get("https://news.google.com/rss/search", params={"q": f"{q} when:1d", "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                               headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            for it in ET.fromstring(res.content).iter("item"):
                try:
                    age_h = (now - parsedate_to_datetime(it.findtext("pubDate")).astimezone(KST)).total_seconds() / 3600
                except Exception:
                    continue
                if age_h > 14:
                    continue
                title = (it.findtext("title") or "").strip()
                src = (it.findtext("source") or "").strip()
                if src and title.endswith(f" - {src}"):
                    title = title[: -len(src) - 3]
                if FLASH_KR_SKIP.search(title) or FLASH_KR_TAG.search(title) or _bio_without_first(title) or FLASH_KR_SKIP_SRC.search(src):
                    continue
                hit = max((nm for nm in names if nm in title), key=len, default=None)
                if not hit or names[hit] in seen_codes:
                    continue
                score = sum(w for pat, w in weight if re.search(pat, title)) + (1 if age_h <= 3 else 0)
                if score < 2:
                    continue
                seen_codes.add(names[hit])
                cands.append((score, title, src, (it.findtext("link") or "").strip(), hit, names[hit]))
        except Exception as e:
            print(f"[morning_news] {q} 실패: {e}")
        time.sleep(0.15)
    cands.sort(key=lambda x: -x[0])
    out = []
    for score, title, src, link, nm, code in cands[:n]:
        tags, mates = [], []
        try:
            d = supabase.table("stock_dictionary").select("tags").eq("code", code).limit(1).execute().data
            tags = (d[0]["tags"] if d else [])[:2]
            if tags:
                m = supabase.table("stock_dictionary").select("name").eq("market", "KR").contains("tags", [tags[0]]).neq("code", code).limit(4).execute().data or []
                mates = [x["name"] for x in m][:3]
        except Exception:
            pass
        out.append(f"· {title} ({src})\n   📎 {link}\n   🏷 {nm}" + (f" · 테마: {'·'.join(tags)}" if tags else "")
                   + (f" · 관련주: {', '.join(mates)}" if mates else "") + f"\n   🔗 {_stock_link(code)}")
    return out


def _build_watch_candidates() -> str:
    """
    장전 리포트용 "오늘 볼 만한 후보" — 예측이 아니라, 밤사이·전일 데이터에서 눈에 띄는 것만 모음:
      ① 밤사이 미국 섹터 힌트 (필라델피아 반도체·나스닥·유가 → 국내 관련 업종)
      ② 전일 강세 테마 TOP3 + 주도주
      ③ 전일 장 마감 후~오늘 아침 실적 공시 종목 (숫자 포함)
      ④ 전일 외국인 순매수 TOP5
    """
    lines = []
    # 📰 오늘 국내 볼 만한 기사 · 📊 최근 실적 공시 · 🌏 전일 외국인 순매수 TOP5 는 뺌 (2026-11-07 JM)
    SKIP = True
    try:
        arts = [] if SKIP else _morning_news_picks()
        if arts:
            lines.append("📰 오늘 국내 볼 만한 기사 (밤사이~아침 · 추천 아님)\n" + "\n\n".join(arts))
    except Exception as e:
        print(f"[brief:candidates] 아침 기사 실패: {e}")
    lines.append("📌 참고 데이터")
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
    # ② 전일 강세 테마 — 아침엔 오늘 등락률이 0이라, 전날 장 마감 뒤 저장해 둔 스냅샷을 씀 (2026-10-23)
    try:
        snap = None
        if (now_kst().hour, now_kst().minute) < (9, 5):
            r = supabase.table("brief_cache").select("data").eq("key", "theme_top_prevday").limit(1).execute().data
            snap = r[0]["data"] if r else None
        tl = []
        for t in (snap or {}).get("themes") or _theme_top_now():
            tl.append(f"· {t['name']} {t['pct']:+.1f}%")
            for x in t["stocks"]:
                tl.append(f"   {x['name']} {x['pct']:+.1f}% 🔗 {_stock_link(x['code'])}")
        if tl:
            lines.append("🔥 전일 강세 테마" + (f" ({snap.get('date')} 마감)" if snap else "") + "\n" + "\n".join(tl))
    except Exception as e:
        print(f"[brief:candidates] 테마 실패: {e}")
    # ③ 전일 장 마감 후 ~ 오늘 아침 실적 공시 — 뺌 (11:00 공시 총정리에서)
    if DART_API_KEY and not SKIP:
        try:
            ltd = _latest_trading_day()
            rows = _dart_list(ltd, now_kst().strftime("%Y%m%d"), max_pages=3)
            res = [r for r in rows if r.get("stock_code") and _classify_disclosure(r.get("report_nm")) == "result"][:5]
            if res:
                el = []
                for r in res:
                    nums = fetch_earnings_numbers(r["rcept_no"])
                    if not nums:
                        continue  # 숫자를 못 읽은 공시는 빼기 (빈 줄만 나오던 문제)
                    el.append(f"· {r['corp_name']} · {_earnings_numbers_text(nums)}\n   🔗 {_stock_link(r['stock_code'])}")
                if el:
                    lines.append("📊 최근 실적 공시\n" + "\n".join(el))
        except Exception as e:
            print(f"[brief:candidates] 실적 공시 실패: {e}")
    # ④ 전일 외국인 순매수 TOP5 — 뺌
    try:
        if SKIP:
            raise StopIteration
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
    except StopIteration:
        pass
    except Exception as e:
        print(f"[brief:candidates] 외국인 순매수 실패: {e}")
    return "\n\n".join(lines)


def build_premarket_brief():
    """📋 장전 리포트 = 어제 수급 한 줄 + 오늘 주요 일정 + 증권가 목표가"""
    now = now_kst()
    parts = [f"📋 장전 리포트 ({_kr_date(now.strftime('%Y-%m-%d'))})\n━━━━━━━━━━━━"]
    # 💰 어제 수급 한 줄 — 뺌 (2026-11-07 JM: 전일 외국인 순매수 제외)
    try:
        raise StopIteration
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
    except StopIteration:
        pass
    except Exception as e:
        print(f"[brief:premarket] 수급 실패: {e}")
    # 🗓 오늘 일정
    try:
        cal = fetch_calendar(1)
        todays = [e for e in cal["events"] if e["date"] == cal["start"]]
        parts.append("🗓 오늘 주요 일정\n" + ("\n".join(_event_line(e).split("\n")[0] for e in todays[:8]) if todays else "· 큰 일정 없음"))
    except Exception as e:
        print(f"[brief:premarket] 일정 실패: {e}")
    # 🆕 신규상장 — 오늘 상장 (+ 월요일엔 이번 주 상장 예정)
    try:
        ipos = fetch_kr_ipo_listings()
        today = now.strftime("%Y-%m-%d")
        todays = [x for x in ipos if x["date"] == today]
        if todays:
            parts.append("🆕 오늘 신규상장\n" + "\n".join(_ipo_line(x) for x in todays)
                         + "\n※ 기관경쟁률↑·의무보유확약↑일수록 첫날 매도 물량이 적은 편")
        if now.weekday() == 0:
            wk_end = (now + timedelta(days=4)).strftime("%Y-%m-%d")
            week = [x for x in ipos if today < x["date"] <= wk_end]
            if week:
                parts.append("🗓 이번 주 상장 예정\n" + "\n".join(f"{_kr_date(x['date'])[5:]} {_ipo_line(x)[2:]}" for x in week))
    except Exception as e:
        print(f"[brief:premarket] 신규상장 실패: {e}")
    # 📌 오늘 볼 만한 후보 (데이터로 고른 것 — 추천 아님, 링크로 바로 종목분석)
    try:
        parts.append(_build_watch_candidates())
    except Exception as e:
        print(f"[brief:premarket] 후보 실패: {e}")
    # 🎯 증권가 목표가 — 뺌 (2026-11-07 JM)
    return "\n\n".join(parts), now.strftime("%Y-%m-%d")


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
    return {"price": f("stck_prpr"), "open": f("stck_oprc"), "high": f("stck_hgpr"), "low": f("stck_lwpr"),
            "pct": f("prdy_ctrt"), "value_eok": f("acml_tr_pbmn") / 1e8, "volume": f("acml_vol")}


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


def _rally_reasons(rows: list) -> dict:
    """🚀 아침 상승 랭킹 종목마다 '왜 올랐나' — 공시(어제 저녁~오늘) · 뉴스(최근 18시간) · 내 종목 사전 테마(+동반 상승)
    ※ 관련 공시·뉴스를 찾아 붙이는 것이지, 반드시 그게 원인이라는 뜻은 아님"""
    from email.utils import parsedate_to_datetime
    codes = [r["code"] for r in rows if r.get("code")]
    tags = {}
    try:
        d = supabase.table("stock_dictionary").select("code,tags,subtags").in_("code", codes).execute().data or []
        tags = {x["code"]: (x.get("tags") or [])[:3] for x in d}
    except Exception:
        pass
    now = now_kst()
    y = (now - timedelta(days=1)).strftime("%Y%m%d")
    out = {}
    for r in rows:
        code, name = r.get("code"), r.get("name")
        parts = []
        # 📢 공시 (DART: 어제~오늘)
        if DART_API_KEY:
            try:
                corp = _dart_corp_code(code)
                if corp:
                    res = requests.get("https://opendart.fss.or.kr/api/list.json",
                                       params={"crtfc_key": DART_API_KEY, "corp_code": corp, "bgn_de": y, "end_de": now.strftime("%Y%m%d"), "page_count": 5},
                                       timeout=8).json()
                    lst = [x for x in (res.get("list") or []) if not re.search(r"임원|주요주주|소유상황|대량보유|기재정정", x.get("report_nm", ""))]
                    if lst:
                        parts.append("📢 공시: " + re.sub(r"\s+", " ", lst[0]["report_nm"]).strip())
            except Exception:
                pass
        # 📰 뉴스 (최근 18시간, 제목 1개)
        try:
            res = requests.get("https://news.google.com/rss/search", params={"q": f'"{name}" when:1d', "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                               headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
            for it in ET.fromstring(res.content).iter("item"):
                try:
                    if (now - parsedate_to_datetime(it.findtext("pubDate")).astimezone(KST)).total_seconds() > 18 * 3600:
                        continue
                except Exception:
                    continue
                title = (it.findtext("title") or "").strip()
                src = (it.findtext("source") or "").strip()
                if src and title.endswith(f" - {src}"):
                    title = title[: -len(src) - 3]
                parts.append(f"📰 뉴스: {title[:60]}")
                break
        except Exception:
            pass
        # 🏷 테마 (내 종목 사전) + 같은 테마로 같이 오른 종목
        t = tags.get(code) or []
        if t:
            mates = [x["name"] for x in rows if x.get("code") != code and set(tags.get(x.get("code"), [])) & set(t)][:3]
            parts.append("🏷 테마: " + "·".join(t) + (f" (동반: {', '.join(mates)})" if mates else ""))
        out[code] = parts or ["❔ 관련 공시·뉴스 없음 → 수급·테마 순환 가능성"]
        time.sleep(0.15)
    return out


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
    # 🏆 2026-11-07 JM: 등락률 TOP 12 + 거래대금 TOP 12 → 두 순위를 더해서 종합 순위 (한쪽에만 있으면 다른 쪽은 13위로)
    rows = [r for r in rows if r["pct"] > 0]
    N = 12
    rp = {r["code"]: i for i, r in enumerate(sorted(rows, key=lambda r: -r["pct"])[:N], 1)}
    rv = {r["code"]: i for i, r in enumerate(sorted(rows, key=lambda r: -r["value_eok"])[:N], 1)}
    for r in rows:
        r["rank_pct"], r["rank_val"] = rp.get(r["code"]), rv.get(r["code"])
        r["score"] = (r["rank_pct"] or N + 1) + (r["rank_val"] or N + 1)
    rows = sorted([r for r in rows if r["code"] in rp or r["code"] in rv], key=lambda r: (r["score"], -r["value_eok"]))[:N]
    if not rows:
        return None, None
    # 🐻 곰발 겹침 표시: ● 곰발 · ○ 곰발 통합 · 🎯 둘 다 (PC 곰발 목록 + 서버 일봉 신호)
    _pcd = _crab_pc.get("data") or {}
    pc_gom = {x["code"] for x in (_pcd.get("big") or [])}
    pc_gomt = {x["code"] for x in (_pcd.get("gomt") or [])}  # HTS 곰발 통합 (PC)

    def gom_mark(code):
        g = code in pc_gom
        gi = code in pc_gomt
        try:
            t = _jm_signal_series(code).get("today") or {}
            g = g or bool(t.get("g"))
            gi = gi or bool(t.get("gi"))
        except Exception:
            pass
        return " 🎯" if (g and gi) else " ●곰발" if g else " ○통합" if gi else ""

    lines = [f"🚀 아침 상승 랭킹 ({now.month}/{now.day} 09:00~{now.strftime('%H:%M')} 기준)\n━━━━━━━━━━━━",
             "등락률 TOP 12 + 거래대금 TOP 12 종합 순위 (두 순위 합) · 거래대금 50억 이상 · ETF·스팩·우선주 제외",
             "● 곰발 · ○ 곰발 통합 · 🎯 둘 다"]
    try:
        reasons = _rally_reasons(rows)
    except Exception as e:
        print(f"[brief:rally] 이유 찾기 실패: {e}")
        reasons = {}
    for i, r in enumerate(rows, start=1):
        extra = ""
        try:
            candles, latest, *_ = fetch_minute_candles_paginated(r["code"], "J", "09:00")
            if latest and latest != now.strftime("%Y%m%d"):
                return None, None  # 오늘 분봉이 없음 = 휴장
            # ⛰ JM 방식: 09:00(상승 시작) ~ 장중 고점 봉까지만 평단 → 고점 이후 하락분이 섞이지 않아 정밀값에 가까움 (2026-10-26)
            pk = max(range(len(candles)), key=lambda i: candles[i]["high"]) if candles else -1
            leg = candles[: pk + 1] if pk >= 0 else candles
            est = estimate_force_avg([{"date": c["time"], "open": c["open"], "close": c["close"], "volume": c["volume"]} for c in leg])
            hi = max(c["high"] for c in candles) if candles else r["high"]
            avg = est[-1]["avg_price"] if est else 0
            if avg:
                extra = (f"\n   세력평단(⛰근사) {avg:,.0f} · 평단 대비 {(r['price'] / avg - 1) * 100:+.1f}%"
                         f" · 장중 고점 대비 {(r['price'] / hi - 1) * 100:+.1f}%")
        except Exception as e:
            print(f"[brief:rally] {r['code']} 분봉 실패: {e}")
        try:  # 🆕 상장 첫날이면 공모가 대비 표시
            ipo = next((x for x in fetch_kr_ipo_listings() if x["date"] == now.strftime("%Y-%m-%d") and x["name"] == _ipo_name(r["name"]) and x.get("offer")), None)
            if ipo:
                extra += f"\n   🆕 상장 첫날 · 공모가 {ipo['offer']:,.0f}원 대비 {(r['price'] / ipo['offer'] - 1) * 100:+.0f}%"
        except Exception:
            pass
        rk = f"등락 {r['rank_pct']}위" if r["rank_pct"] else "등락 -"
        rk += f" · 대금 {r['rank_val']}위" if r["rank_val"] else " · 대금 -"
        lines.append(
            f"\n{i}. {r['name']}{gom_mark(r['code'])} {r['pct']:+.1f}% · {_fmt_eok(r['value_eok'], signed=False)} ({rk})"
            f"\n   시가대비 {r['from_open']:+.1f}%{extra}"
            + "".join(f"\n   {x}" for x in reasons.get(r["code"], []))
            + f"\n   🔗 {FRONTEND_URL}/stock-analysis?code={r['code']}"
        )
        time.sleep(0.1)
    lines.append("\n※ 평단 대비 +가 작고 고점 대비 -가 적당하면 눌림목 후보 — 링크로 분봉·세력평단 확인"
                 "\n※ 📢·📰는 관련 공시·뉴스를 찾아 붙인 것 (원인이 아닐 수도 있어요) · 🏷는 내 종목 사전 키워드")
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


# ── 📢 실적발표 예고·IR 모아보기 (16:00 · 20:10) ──
def build_plan_digest():
    if not DART_API_KEY:
        return None, None
    text, rows = build_earnings_digest(days=2, only_new=True, types=("plan",), sent_kind="dart_plan")
    if not text:
        return None, None
    for r in rows:  # 보낸 것으로 기록 (정정 중복 포함) — 실제 발송 성공 여부와 상관없이 다음 회차 중복 방지
        _mark_brief_sent("dart_plan", r["rcept_no"])
    return text.replace("📊 실적 공시 알림", "📢 실적발표 예고·IR 모아보기"), now_kst().strftime("%Y-%m-%d")


# ── 🌍 세계 증시 이슈 (11:00 · 15:00 · 20:20) ──
# 구글 뉴스 RSS(무료, 키 없음)에서 지역별로 최근 기사를 모아 "주가에 영향 줄 만한" 단어가 들어간 것 위주로 추림.
# AI 요약 없이 제목·출처·링크만 (비용 0). 한 번 보낸 기사는 다음 회차에 다시 안 보냄.
NEWS_REGIONS = [
    # 🌐 증시 말고 세계 정세 자체 (전쟁·외교·정상회담·선거·재난 등) — 2026-10-14 요청으로 맨 위에 추가
    ("🌐 세계 주요뉴스", "국제 정세 OR 정상회담 OR 전쟁 OR 외교 OR 대선 OR 제재 OR 휴전"),
    ("🇺🇸 미국", "뉴욕증시 OR 미국증시 OR 연준 OR 나스닥 OR 미국 국채"),
    ("🇰🇷 국내", "코스피 OR 코스닥 OR 외국인 순매도 OR 외국인 순매수"),
    ("🇨🇳 중국", "중국 증시 OR 상하이증시 OR 중국 반도체 OR 중국 경제 OR 홍콩증시"),
    ("🇯🇵 일본", "일본증시 OR 닛케이 OR 일본은행 OR 엔화"),
    ("🌏 그외 아시아", "대만 증시 OR TSMC OR 인도 증시 OR 베트남 증시 OR 동남아"),
    ("🇪🇺 유럽", "유럽증시 OR 유로존 OR ECB OR 프랑스 국채 OR 독일 경제"),
    ("🌍 중동·원자재", "중동 OR 국제유가 OR 이란 OR 이스라엘 OR OPEC"),
    ("💱 환율·금리", "원달러 환율 OR 국채금리 OR 달러 강세"),
]
NEWS_IMPACT_WORDS = [
    "급락", "급등", "폭락", "폭등", "쇼크", "사상 최고", "최저", "관세", "금리", "연준", "인상", "인하", "제재", "전쟁",
    "공습", "반도체", "실적", "어닝", "부진", "충격", "위기", "디폴트", "국채", "유가", "환율", "규제", "수출", "AI",
]
NEWS_MAJOR_SOURCES = ["연합뉴스", "한국경제", "매일경제", "머니투데이", "이데일리", "서울경제", "뉴스1", "조선비즈", "한경", "블룸버그", "로이터"]


def _google_news(query: str, hours: int = 8) -> list:
    from email.utils import parsedate_to_datetime

    res = requests.get(
        "https://news.google.com/rss/search",
        params={"q": f"{query} when:{hours}h", "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=10,
    )
    res.raise_for_status()
    root = ET.fromstring(res.content)
    out = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        src = (it.findtext("source") or "").strip()
        if src and title.endswith(f" - {src}"):
            title = title[: -len(src) - 3].strip()
        try:
            pub = parsedate_to_datetime(it.findtext("pubDate")).astimezone(KST)
        except Exception:
            pub = None
        out.append({"title": title, "source": src, "link": (it.findtext("link") or "").strip(), "pub": pub})
    return out


def _news_score(n: dict) -> float:
    t = n["title"]
    score = sum(2 for w in NEWS_IMPACT_WORDS if w in t)
    score += 1.5 if any(sname in n["source"] for sname in NEWS_MAJOR_SOURCES) else 0
    if n.get("pub"):
        score += max(0.0, 3 - (now_kst() - n["pub"]).total_seconds() / 3600 / 2)  # 최근일수록 가산
    return score


def build_world_news_brief(per_region: int = 2, hours: int = 8):
    now = now_kst()
    parts = [f"🌍 세계 증시 이슈 ({now.month}/{now.day} {now.strftime('%H:%M')})\n━━━━━━━━━━━━"]
    seen_titles, sent_links = set(), []
    for label, q in NEWS_REGIONS:
        try:
            items = _google_news(q, hours)
        except Exception as e:
            print(f"[news] {label} 실패: {e}")
            continue
        picked = []
        for n in sorted(items, key=_news_score, reverse=True):
            key = re.sub(r"[^가-힣A-Za-z0-9]", "", n["title"])[:22]
            if not n["title"] or key in seen_titles:
                continue
            if _brief_already_sent("news", n["link"][:200]):
                continue
            seen_titles.add(key)
            picked.append(n)
            if len(picked) >= per_region:
                break
        if picked:
            lines = [label]
            for n in picked:
                tm = n["pub"].strftime("%H:%M") if n.get("pub") else ""
                lines.append(f"· {n['title']} ({n['source']} {tm})\n  🔗 {n['link']}")
                sent_links.append(n["link"][:200])
            parts.append("\n".join(lines))
    if len(parts) == 1:
        return None, None
    for l in sent_links:
        _mark_brief_sent("news", l)
    parts.append("※ 제목만 모은 거라, 영향이 클 것 같은 기사는 링크로 내용 확인하세요.")
    return "\n\n".join(parts), now.strftime("%Y-%m-%d")


# ── 📝 오늘의 마감 리포트 초안 (20:40) ──
# 매일 장 마감 후 리포트를 쓰기 위한 "재료 정리본". 지수·수급·업종·테마·시장심리·관심종목·뉴스·내일 일정을
# 한 장으로 모으고, 숫자 기준으로 짚어볼 포인트를 덧붙임 (AI 문장 생성 아님, 비용 0).
def build_closing_report():
    now = now_kst()
    ltd = _latest_trading_day()
    if ltd != now.strftime("%Y%m%d"):
        return None, None
    day = f"{ltd[:4]}-{ltd[4:6]}-{ltd[6:]}"
    P = [f"📝 오늘의 마감 리포트 초안 ({_kr_date(day)})\n━━━━━━━━━━━━"]
    checks = []
    # 1) 지수
    try:
        il = []
        for code, nm in (("1001", "코스피"), ("2001", "코스닥")):
            df = _krx_df(stock.get_index_ohlcv_by_date, (now - timedelta(days=10)).strftime("%Y%m%d"), ltd, code)
            c0, c1 = float(df["종가"].iloc[-1]), float(df["종가"].iloc[-2])
            pct = (c0 / c1 - 1) * 100
            il.append(f"{'🔴▲' if pct > 0 else '🔵▼'} {nm} {c0:,.2f} ({pct:+.2f}%)")
        P.append("① 지수\n" + "\n".join(il))
    except Exception as e:
        print(f"[closing] 지수 실패: {e}")
    # 2) 수급
    try:
        r = supabase.table("market_flow_trend").select("*").eq("market", "KOSPI").eq("trade_date", day).limit(1).execute().data
        if r:
            r = r[0]
            fl = []
            for nm, f in (("외국인", "foreign_amt"), ("기관", "institution"), ("개인", "individual")):
                v = float(r.get(f) or 0)
                st = _streak("KOSPI", f, v)
                fl.append(f"{nm} {_fmt_eok(v)}" + (f" ⟨{st}일 연속⟩" if st >= 2 else ""))
                if nm == "외국인" and st >= 5:
                    checks.append(f"외국인 코스피 {st}거래일 연속 순{'매수' if v > 0 else '매도'} — 추세 지속 여부 확인")
            P.append("② 코스피 수급\n" + " · ".join(fl))
    except Exception as e:
        print(f"[closing] 수급 실패: {e}")
    # 2-1) 선물 투자자별
    try:
        fu = fetch_futures_investor()
        P.append("②-1 코스피200 선물\n" + " · ".join(
            f"{nm} {fu[k + '_qty']:+,.0f}계약({_fmt_eok(fu[k + '_eok'])})"
            for nm, k in (("외국인", "foreign"), ("기관", "institution"), ("개인", "individual"))))
    except Exception as e:
        print(f"[closing] 선물 실패: {e}")
    # 3) 업종 강약
    try:
        tm = supabase.table("sector_treemap").select("market,name,pct,trading_value,updated_at").execute().data or []
        # 오늘 갱신된 업종 지도만 사용 (예전 값이면 엉뚱한 등락률이 나와서 건너뜀)
        tm = [t for t in tm if (t.get("trading_value") or 0) >= 300 and str(t.get("updated_at") or "")[:10] == day]
        if tm:
            top = sorted(tm, key=lambda t: -t["pct"])[:3]
            bot = sorted(tm, key=lambda t: t["pct"])[:3]
            mk = lambda m: "코스피" if m == "KOSPI" else "코스닥"
            P.append("③ 업종 강약 (거래대금 300억 이상)\n강세: " + ", ".join(f"{t['name']}({mk(t['market'])}) {t['pct']:+.1f}%" for t in top)
                     + "\n약세: " + ", ".join(f"{t['name']}({mk(t['market'])}) {t['pct']:+.1f}%" for t in bot))
    except Exception as e:
        print(f"[closing] 업종 실패: {e}")
    # 4) 테마
    try:
        th = sorted(fetch_naver_theme_list(limit=25), key=lambda t: -t["change_pct"])[:3]
        P.append("④ 강세 테마\n" + ", ".join(f"{t['theme_name']} {t['change_pct']:+.1f}%" for t in th))
    except Exception as e:
        print(f"[closing] 테마 실패: {e}")
    # 5) 외국인 순매수 TOP5
    try:
        rows = (supabase.table("investor_flow").select("name,amount,streak_days").eq("investor_type", "foreign")
                .eq("period", "daily").eq("side", "buy").order("rank").limit(5).execute().data) or []
        if rows:
            P.append("⑤ 외국인 순매수 TOP5\n" + ", ".join(f"{r['name']} {_fmt_eok(r['amount'])}" for r in rows))
    except Exception as e:
        print(f"[closing] TOP5 실패: {e}")
    # 6) 시장심리·공포
    try:
        ms = fetch_market_sentiment()
        last = (ms.get("rows") or [])[-1] if ms.get("rows") else None
        fear = fear_indices_endpoint()
        parts = []
        if last and last.get("credit") and last.get("deposit"):
            ratio = last["credit"] / last["deposit"] * 100
            parts.append(f"신용/예탁 {ratio:.1f}% ({last['date'][5:]} 기준)")
            if ratio >= 40:
                checks.append("신용/예탁 비율 40% 이상 — 과열, 급락 시 반대매매 주의")
        vk = fear.get("vkospi") if isinstance(fear, dict) else None
        if vk:
            v = vk["value"]
            lvl = "🔴 고공포" if v >= 40 else "🟠 경계" if v >= 30 else "🟡 보통" if v >= 20 else "🟢 안정"
            parts.append(f"VKOSPI {v} {lvl} (60일 평균 {vk['avg60']})")
            if vk["value"] >= vk["avg60"] * 1.3:
                checks.append("VKOSPI가 평균보다 30% 이상 높음 — 공포 구간, 변동성 확대")
        cnn = fear.get("cnn") if isinstance(fear, dict) else None
        if cnn:
            parts.append(f"CNN 공포·탐욕 {cnn['score']} ({cnn['rating']})")
        if parts:
            P.append("⑥ 시장심리\n" + " · ".join(parts))
    except Exception as e:
        print(f"[closing] 심리 실패: {e}")
    # 7) 관심종목 오늘 성적
    try:
        wl = supabase.table("watchlist").select("code,name").execute().data or []
        if wl:
            df = _krx_df(stock.get_market_ohlcv_by_ticker, ltd, market="ALL")
            res = []
            for w in wl:
                if w["code"] in df.index:
                    res.append((float(df.loc[w["code"], "등락률"]), w["name"]))
            res.sort(reverse=True)
            if res:
                P.append("⑦ 관심종목 (등락률 순)\n" + ", ".join(f"{n} {p:+.1f}%" for p, n in res))
    except Exception as e:
        print(f"[closing] 관심종목 실패: {e}")
    # 8) 내일 주요 일정
    try:
        cal = fetch_calendar(4)
        nxt = sorted({e["date"] for e in cal["events"] if e["date"] > day})
        if nxt:
            evs = [e for e in cal["events"] if e["date"] == nxt[0] and (e["major"] or e["kind"] == "kr_macro")][:6]
            if evs:
                P.append(f"⑧ 다음 일정 ({_date_head(nxt[0])})\n" + "\n".join(_event_line(e).split("\n")[0] for e in evs))
    except Exception as e:
        print(f"[closing] 일정 실패: {e}")
    if checks:
        P.append("🔎 체크 포인트\n" + "\n".join(f"· {c}" for c in checks))
    P.append("✍️ 내 생각 (직접 작성)\n· 오늘 시장 한 줄 요약:\n· 내일 전략:\n· 관심종목 대응:")
    return "\n\n".join(P), day


def _build_disclosure_batch(label: str):
    """📢 JM 공시 총정리 (2026-11-07) — 실적 발표·예고·IR을 한 번에 모아서 (예전 10분마다 실시간 → 11:00·17:00 두 번)"""
    if not DART_API_KEY:
        return None, None
    text, rows = build_earnings_digest(days=1, only_new=True, types=("result", "plan"), sent_kind="dart_earn")
    if not text:
        return None, None
    for r in rows:
        _mark_brief_sent("dart_earn", r["rcept_no"])
    return text.replace("📊 실적 공시 알림", f"📢 {label} 공시 총정리"), now_kst().strftime("%Y-%m-%d")


def _build_jm_news():
    text, _ = build_world_news_digest()
    return text, now_kst().strftime("%Y-%m-%d")


BRIEF_BUILDERS = {
    "jmnews_07": _build_jm_news,
    "jmnews_12": _build_jm_news,
    "jmnews_18": _build_jm_news,
    "disc_am": lambda: _build_disclosure_batch("오전"),
    "disc_pm": lambda: _build_disclosure_batch("오후"),
    "worldnews": lambda: build_world_news_digest(),
    "us_pre": lambda: build_us_pre_brief(),
    "us_open30": lambda: build_us_open30_brief(),
    "closing": build_closing_report,
    "news_11": build_world_news_brief,
    "news_15": build_world_news_brief,
    "news_2020": build_world_news_brief,
    "ir_16": build_plan_digest,
    "ir_2010": build_plan_digest,
    "rally": build_rally_brief,
    "theme_am": build_theme_am_brief,
    "morning": build_morning_brief,
    "targets": build_premarket_brief,
    "schedule": build_schedule_brief,
    "flow": build_flow_brief,
    "theme": build_theme_brief,
}


def _brief_claim(kind: str, day: str) -> bool:
    """🔒 같은 브리핑을 두 번 만들지 않게 '만드는 중' 표시를 DB에 먼저 박음 (2026-10-27: 14:00 일정이 2번 옴)
    — 만드는 데 몇 분 걸리는 브리핑(일정·IR 원문 읽기)이 겹쳐 돌면서 둘 다 보내졌음. 이미 표시가 있으면 False"""
    try:
        supabase.table("telegram_sent_log").insert({"kind": f"lock:{kind}", "ref_date": day}).execute()
        return True
    except Exception:
        pass
    # 표시가 15분 넘게 남아 있는데 실제로는 안 보내졌으면(만들다 서버가 재시작된 경우 등) 풀고 다시
    try:
        r = supabase.table("telegram_sent_log").select("sent_at").eq("kind", f"lock:{kind}").eq("ref_date", day).limit(1).execute().data
        if r and not _brief_already_sent(f"done:{kind}", day):
            at = datetime.fromisoformat(str(r[0]["sent_at"]).replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - at).total_seconds() > 900:
                _brief_unclaim(kind, day)
                supabase.table("telegram_sent_log").insert({"kind": f"lock:{kind}", "ref_date": day}).execute()
                return True
    except Exception:
        pass
    return False


def _brief_unclaim(kind: str, day: str):
    try:
        supabase.table("telegram_sent_log").delete().eq("kind", f"lock:{kind}").eq("ref_date", day).execute()
    except Exception:
        pass


def _run_telegram_briefs():
    """daily_jobs_worker가 5분마다 부름 — 정해진 시각이 지났고 아직 안 보낸 브리핑만 보냄"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    now = now_kst()
    wd = now.weekday()  # 0=월 ... 6=일
    today0 = now.strftime("%Y-%m-%d")
    # 🔨 미리 만들기: 정각 몇 분 전에 뒤에서 만들어 둠 (정각엔 보내기만)
    for kind, (h, m) in BRIEF_SCHEDULE.items():
        lead = BRIEF_LEAD.get(kind, 0)
        start_at = now.replace(hour=h, minute=m, second=0, microsecond=0) - timedelta(minutes=lead)
        if not lead or now < start_at or (now.hour, now.minute) >= (h, m) or (kind, today0) in _prebuilt:
            continue
        if (kind != "morning" and wd >= 5) or (kind == "morning" and wd >= 5):
            continue
        if kind in ("theme_am", "rally") and not _is_trading_today():
            continue
        if _brief_already_sent(kind, today0) or _brief_already_sent(f"done:{kind}", today0):
            continue
        _prebuilt[(kind, today0)] = "building"

        def _pre(k=kind, d=today0):
            try:
                _prebuilt[(k, d)] = BRIEF_BUILDERS[k]()
                print(f"[brief] {k} 미리 만들어 둠")
            except Exception as e:
                _prebuilt.pop((k, d), None)
                print(f"[brief] {k} 미리 만들기 실패: {e}")

        threading.Thread(target=_pre, daemon=True).start()
    for kind, (h, m) in BRIEF_SCHEDULE.items():
        if (now.hour, now.minute) < (h, m) or now.hour >= 23:
            continue
        if _prebuilt.get((kind, today0)) == "building" and (now.hour * 60 + now.minute) - (h * 60 + m) < 10:
            continue  # 아직 만드는 중 — 15초 뒤 다시 (10분 넘게 안 끝나면 그냥 직접 만들기)
        if kind == "morning" and wd not in (0, 1, 2, 3, 4):  # 월~금 아침만 (주말 알림 없음 — 월요일 아침에 금요일 미국·유럽 마감을 받음, 2026-10-26)
            continue
        if kind == "morning" and (now.hour, now.minute) >= (9, 0):
            # ⚠️ 야후 일봉 확정이 늦으면 하루 종일 재시도하다가 밤 10시 반(미국장 개장)에 엉뚱하게 보낸 일이 있어서
            # (2026-10-14) 아침 9시까지 못 보내면 그날은 건너뜀
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
            if kind != "morning" and _brief_already_sent(kind, now.strftime("%Y-%m-%d")):
                continue
            today = now.strftime("%Y-%m-%d")
            if not _brief_claim(kind, today):
                continue  # 다른 쪽에서 지금 만들고 있거나 이미 보냄
            try:
                pre = _prebuilt.pop((kind, today), None)
                text, ref = pre if isinstance(pre, tuple) else BRIEF_BUILDERS[kind]()
            except Exception:
                _brief_unclaim(kind, today)
                raise
            if not text or not ref or _brief_already_sent(kind, ref):
                _brief_unclaim(kind, today)  # 보낼 게 없으면 표시를 풀어 다음 주기에 다시 (모닝 데이터 늦을 때 등)
                continue
            if send_brief(kind, text):
                _mark_brief_sent(kind, ref)
                _mark_brief_sent(f"done:{kind}", today)  # 오늘 보냄 표시 (잠금 정리용 — 모닝처럼 기준 날짜가 다른 브리핑도)
                print(f"[brief] {kind} 발송 완료 ({ref})")
                if kind == "morning" and _flash_state.get("morning_keys"):
                    used = set(_flash_state.pop("morning_keys"))
                    for k in used:
                        _flash_mark(k)
                    _flash_state["pending"] = [i for i in _flash_state.get("pending", []) if i["key"] not in used]
            else:
                _brief_unclaim(kind, today)
                print(f"[brief] {kind} 텔레그램 발송 실패 — 다음 주기에 다시 시도")
        except Exception as e:
            print(f"[brief] {kind} 만들기 실패: {e}")


@app.get("/api/brief")
def brief_preview_endpoint(kind: str = "flow", send: bool = False):
    """브리핑 미리보기 — kind: morning/targets(장전 리포트)/schedule/flow/theme. send=true면 텔레그램으로도 보냄(기록 안 남김)"""
    if kind not in BRIEF_BUILDERS:
        return {"ok": False, "error": f"kind는 {'/'.join(BRIEF_BUILDERS)} 중 하나예요."}
    try:
        text, ref = BRIEF_BUILDERS[kind]()
        sent = send_brief(kind, text) if (send and text) else False
        send_error = _tg_last_error["msg"] if (send and text and not sent) else None
        return {"ok": True, "kind": kind, "ref_date": ref, "sent": sent, "send_error": send_error, "bot": BRIEF_BOT.get(kind) or ("report" if bot("report")[0] else "main"), "text": text}
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
            yoy = qoq = prev = None
            if len(nums) >= 5:  # 당기, 전기, 전기대비%, 전년동기, 전년동기대비%
                qoq, yoy = nums[2], nums[4]
                prev = nums[3] if isinstance(nums[3], float) else None
            elif len(nums) >= 2 and isinstance(nums[1], float) and nums[1]:  # 당기, 전기(또는 전년)
                prev = nums[1]
                yoy = (cur / nums[1] - 1) * 100 if nums[1] > 0 else ("흑자전환" if cur > 0 > nums[1] else None)
            found[key] = {"cur": cur * mul if mul else None, "raw": cur, "qoq": qoq, "yoy": yoy, "prev": prev}
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


def _profit_badge(nums: dict) -> str:
    """실적 공시 흑자·적자 색 표시 (2026-10-28) — 영업이익 기준(없으면 순이익). 전년 동기와 비교해 전환도 표시"""
    v = (nums or {}).get("영업이익") or (nums or {}).get("순이익")
    if not v or v.get("raw") is None:
        return ""
    cur, prev = v["raw"], v.get("prev")
    if prev is not None and cur > 0 > prev:
        return "⭐흑자전환"
    if prev is not None and cur < 0 < prev:
        return "⚠️적자전환"
    if cur < 0 and prev is not None and prev < 0:
        return "🔵적자지속"
    return "🔴흑자" if cur > 0 else "🔵적자" if cur < 0 else ""


def _disclosure_line(r: dict, watch: set) -> str:
    star = "⭐" if r.get("stock_code") in watch else ""
    mkt = {"Y": "코스피", "K": "코스닥"}.get(r.get("corp_cls"), "")
    name = re.sub(r"\s+", " ", r.get("report_nm") or "").strip()
    nums_line = ""
    badge = ""
    if _classify_disclosure(r.get("report_nm")) == "result":
        nums = fetch_earnings_numbers(r.get("rcept_no"))
        if nums:
            nums_line = f"\n   💰 {_earnings_numbers_text(nums)}"
            b = _profit_badge(nums)
            badge = f"{b} " if b else ""
    return (f"{badge}{star}{r.get('corp_name')}" + (f" ({mkt})" if mkt else "") + f"\n   {name}{nums_line}"
            f"\n   🔗 https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.get('rcept_no')}")


def _dedup_by_corp(rows: list) -> list:
    """같은 회사의 원공시와 [기재정정]이 따로 잡히면 가장 최근(접수번호 큰) 것 하나만 남김"""
    best = {}
    for r in rows:
        k = (r.get("stock_code"), _classify_disclosure(r.get("report_nm")))
        if k not in best or str(r.get("rcept_no")) > str(best[k].get("rcept_no")):
            best[k] = r
    keep = {id(v) for v in best.values()}
    return [r for r in rows if id(r) in keep]


def build_earnings_digest(days: int = 1, only_new: bool = False, types: tuple = ("result", "plan"), sent_kind: str = "dart_earn"):
    now = now_kst()
    bgn = (now - timedelta(days=days - 1)).strftime("%Y%m%d")
    rows = _dart_list(bgn, now.strftime("%Y%m%d"))
    rows = [r for r in rows if r.get("stock_code") and _classify_disclosure(r.get("report_nm"))]
    rows = [r for r in rows if _classify_disclosure(r.get("report_nm")) in types]
    if only_new:
        rows = [r for r in rows if not _brief_already_sent(sent_kind, r["rcept_no"])]
    all_rows = rows
    rows = _dedup_by_corp(rows)
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
        return None, all_rows
    parts = [f"📊 실적 공시 알림 ({now.strftime('%m/%d %H:%M')})\n━━━━━━━━━━━━"]
    if results:
        parts.append(f"📊 실적 발표 ({len(results)}건)\n" + "\n".join(_disclosure_line(r, watch) for r in results[:25])
                     + (f"\n외 {len(results) - 25}건" if len(results) > 25 else ""))
    if plans:
        parts.append(f"📢 실적 발표 예고·IR ({len(plans)}건)\n" + "\n".join(_disclosure_line(r, watch) for r in plans[:15])
                     + (f"\n외 {len(plans) - 15}건" if len(plans) > 15 else ""))
    return "\n\n".join(parts), all_rows


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
        # 📊 주가에 바로 영향 주는 "실적 발표"만 실시간(10분마다). 예고·IR은 16:00·20:10에 모아서 보냄
        text, rows = build_earnings_digest(days=1, only_new=True, types=("result",))
    except Exception as e:
        print(f"[earnings] 공시 확인 실패: {e}")
        return
    if text and send_telegram(text, *bot("flash")):
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
    if c["data"] and time.time() - c["at"] < 1 * 3600:  # 1시간마다 새로 확인 (협회 발표 시각이 날마다 조금씩 달라서)
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


# ══════════════════════════════════════════════════════════════════════════
# 🗺 시장분석 지도·업종 분류·업종별 수급·투자자별 TOP30 — PC 프로그램 → 클라우드 이전 (2026-10-14)
# ──────────────────────────────────────────────────────────────────────────
# 예전엔 PC 프로그램이 채워서 PC를 안 켠 8/15 이후로 멈춰 있었음. 이제 서버가 매일 자동으로 채움.
#   · sector_stocks / sector_treemap : 네이버 업종(반도체와반도체장비 등) 구성 종목 + 등락률·거래대금 (15:50 이후)
#   · sector_flow_trend              : 업종별 외국인·기관·개인·연기금 순매수 (KRX 종목별 순매수를 업종으로 합산, 16:20 이후)
#   · investor_flow / _summary      : 외국인·기관·연기금 순매수/순매도 TOP30 (당일·1주) + 연속 순매수 일수
# 테이블 모양은 기존 화면이 읽던 그대로 맞춰서, 화면 코드는 안 바꿔도 됨.
# ══════════════════════════════════════════════════════════════════════════
_sector_last_raw = {"list": None}


def _naver_industry_list() -> list:
    res = requests.get("https://m.stock.naver.com/api/stocks/industry", headers=_naver_stock_headers(),
                       params={"page": 1, "pageSize": 100}, timeout=10)
    _sector_last_raw["list"] = f"HTTP {res.status_code}: {res.text[:500]}"
    res.raise_for_status()
    data = res.json()
    groups = None
    if isinstance(data, dict):
        for k in ("groups", "industries", "list", "result", "items"):
            if isinstance(data.get(k), list):
                groups = data[k]
                break
    elif isinstance(data, list):
        groups = data
    out = []
    for g in groups or []:
        if not isinstance(g, dict):
            continue
        no = g.get("no") or g.get("industryNo") or g.get("id")
        name = g.get("name") or g.get("industryName")
        if no and name:
            out.append({"no": no, "name": name})
    return out


def _naver_industry_stocks(no) -> list:
    out = []
    for page in range(1, 6):
        res = requests.get(f"https://m.stock.naver.com/api/stocks/industry/{no}", headers=_naver_stock_headers(),
                           params={"page": page, "pageSize": 100}, timeout=10)
        if not res.ok:
            break
        stocks = (res.json() or {}).get("stocks") or []
        out += stocks
        if len(stocks) < 100:
            break
    return out


def _kosdaq_codes() -> set:
    try:
        return set(stock.get_market_ticker_list(_latest_trading_day(), market="KOSDAQ"))
    except Exception as e:
        print(f"[sector_map] 코스닥 종목 목록 실패: {e}")
        return set()


def sync_sector_map(dry: bool = False) -> dict:
    industries = _naver_industry_list()
    if not industries:
        raise ValueError(f"네이버 업종 목록이 비어 있어요 — 응답: {_sector_last_raw['list']}")
    if dry:
        industries = industries[:3]  # 미리보기는 3개 업종만 빨리
    kosdaq = _kosdaq_codes()
    now_iso = now_kst().isoformat()
    stock_rows, agg = [], {}

    def f(v):
        try:
            return float(str(v).replace(",", ""))
        except (TypeError, ValueError):
            return 0.0

    for ind in industries:
        try:
            stocks = _naver_industry_stocks(ind["no"])
        except Exception as e:
            print(f"[sector_map] {ind['name']} 실패: {e}")
            continue
        for st in stocks:
            code = st.get("itemCode")
            if not code:
                continue
            exch = (st.get("stockExchangeType") or {}).get("code") if isinstance(st.get("stockExchangeType"), dict) else None
            market = "KOSDAQ" if (exch in ("KQ", "KOSDAQ") or code in kosdaq) else "KOSPI"
            pct = f(st.get("fluctuationsRatio"))
            val = _to_eok_from_naver(f(st.get("accumulatedTradingValueRaw")), f(st.get("closePriceRaw")),
                                     f(st.get("accumulatedTradingVolumeRaw")))
            stock_rows.append({"market": market, "sector_name": ind["name"], "code": code, "name": st.get("stockName") or code,
                               "pct": round(pct, 2), "trading_value": round(val, 1), "updated_at": now_iso})
            a = agg.setdefault((market, ind["name"]), {"val": 0.0, "wpct": 0.0})
            a["val"] += val
            a["wpct"] += pct * val
        time.sleep(0.1)
    tree_rows = [
        {"market": m, "name": n, "ticker": "", "pct": round(a["wpct"] / a["val"], 2) if a["val"] else 0.0,
         "trading_value": round(a["val"], 1), "updated_at": now_iso}
        for (m, n), a in agg.items()
    ]
    if dry:
        return {"ok": True, "dry_run": True, "industries_sampled": len(industries), "stocks": len(stock_rows), "treemap": len(tree_rows),
                "sample_stocks": stock_rows[:3], "sample_treemap": sorted(tree_rows, key=lambda r: -r["trading_value"])[:5]}
    if len(stock_rows) < 1000:
        raise ValueError(f"업종 구성 종목이 {len(stock_rows)}개뿐이라 저장 안 함 (네이버 응답 확인 필요)")
    supabase.table("sector_stocks").delete().neq("code", "").execute()
    for i in range(0, len(stock_rows), 500):
        supabase.table("sector_stocks").insert(stock_rows[i:i + 500]).execute()
    supabase.table("sector_treemap").delete().neq("name", "").execute()
    supabase.table("sector_treemap").insert(tree_rows).execute()
    return {"ok": True, "stocks": len(stock_rows), "treemap": len(tree_rows)}


INVESTOR_TYPES = {"foreign": "외국인", "institution": "기관합계", "pension": "연기금"}


def _net_purchases(fromd: str, tod: str, investor: str):
    df = _krx_df(stock.get_market_net_purchases_of_equities, fromd, tod, "ALL", investor)
    if df is None or df.empty:
        return None
    col = "순매수거래대금" if "순매수거래대금" in df.columns else df.columns[-1]
    return df[[col] + (["종목명"] if "종목명" in df.columns else [])].rename(columns={col: "net"})


def sync_investor_top30() -> dict:
    dates = [str(r["trade_date"]).replace("-", "") for r in (
        supabase.table("market_flow_trend").select("trade_date").eq("market", "ALL")
        .order("trade_date", desc=True).limit(10).execute().data or [])]
    if len(dates) < 5:
        raise ValueError("거래일 목록이 부족해요 (시장 수급 데이터 확인)")
    today, week_start = dates[0], dates[4]
    now_iso = now_kst().isoformat()
    flow_rows, sum_rows = [], []
    for key, inv in INVESTOR_TYPES.items():
        daily_by_day = {}
        for d in dates:  # 연속 순매수 일수 계산용 (최근 10거래일)
            df = _net_purchases(d, d, inv)
            if df is not None:
                daily_by_day[d] = df["net"]
            time.sleep(0.3)
        for period, (fd, td) in {"daily": (today, today), "weekly": (week_start, today)}.items():
            df = _net_purchases(fd, td, inv)
            if df is None:
                continue
            df = df.copy()
            df["eok"] = df["net"] / 1e8
            buys = df.sort_values("eok", ascending=False).head(30)
            sells = df.sort_values("eok").head(30)
            for side, part in (("buy", buys), ("sell", sells)):
                for rank, (code, r) in enumerate(part.iterrows(), start=1):
                    sign = 1 if side == "buy" else -1
                    streak = 0
                    for d in dates:
                        v = daily_by_day.get(d)
                        if v is None or code not in v.index or v[code] * sign <= 0:
                            break
                        streak += 1
                    nm = r.get("종목명") if isinstance(r.get("종목명"), str) else _safe_ticker_name(code, allow_remote=False)
                    flow_rows.append({"investor_type": key, "period": period, "side": side, "rank": rank, "code": code,
                                      "name": nm, "streak_days": streak, "amount": round(float(r["eok"]), 1), "updated_at": now_iso})
            label = today if period == "daily" else f"{week_start} ~ {today}"
            sum_rows.append({"investor_type": key, "period": period, "label": label,
                             "total_buy": round(float(buys["eok"].sum()), 1), "total_sell": round(abs(float(sells["eok"].sum())), 1),
                             "updated_at": now_iso})
    if not flow_rows:
        raise ValueError("KRX 투자자별 순매수를 가져오지 못했어요")
    supabase.table("investor_flow").delete().neq("code", "").execute()
    for i in range(0, len(flow_rows), 500):
        supabase.table("investor_flow").insert(flow_rows[i:i + 500]).execute()
    supabase.table("investor_flow_summary").delete().neq("label", "").execute()
    supabase.table("investor_flow_summary").insert(sum_rows).execute()
    return {"ok": True, "rows": len(flow_rows)}


def sync_sector_flow() -> dict:
    """업종별 투자자 순매수 = KRX 종목별 순매수(외국인·기관·개인·연기금)를 sector_stocks 업종으로 합산"""
    ltd = _latest_trading_day()
    day = f"{ltd[:4]}-{ltd[4:6]}-{ltd[6:]}"
    mapping = {}
    start = 0
    while True:
        rows = supabase.table("sector_stocks").select("code, market, sector_name").range(start, start + 999).execute().data or []
        for r in rows:
            mapping[r["code"]] = (r["market"], r["sector_name"])
        if len(rows) < 1000:
            break
        start += 1000
    totals = {}
    for field, inv in (("foreign_amt", "외국인"), ("institution", "기관합계"), ("individual", "개인"), ("pension", "연기금")):
        df = _net_purchases(ltd, ltd, inv)
        if df is None:
            continue
        for code, v in df["net"].items():
            key = mapping.get(code)
            if key:
                t = totals.setdefault(key, {"foreign_amt": 0.0, "institution": 0.0, "individual": 0.0, "pension": 0.0})
                t[field] += float(v) / 1e8
        time.sleep(0.3)
    rows = [{"trade_date": day, "market": m, "sector_name": n, **{k: round(v, 1) for k, v in t.items()}} for (m, n), t in totals.items()]
    if not rows:
        raise ValueError("업종별 수급을 계산하지 못했어요")
    supabase.table("sector_flow_trend").delete().eq("trade_date", day).execute()
    supabase.table("sector_flow_trend").insert(rows).execute()
    return {"ok": True, "date": day, "sectors": len(rows)}


def _ensure_sector_jobs():
    """평일 15:50 이후 업종 지도, 16:20 이후 TOP30·업종별 수급 — 하루 1번씩 (실패하면 5분 뒤 다시)"""
    now = now_kst()
    if now.weekday() >= 5 or now.hour >= 22:
        return
    today = now.strftime("%Y-%m-%d")
    jobs = [("job_sector_map", (15, 50), sync_sector_map), ("job_investor_top30", (16, 20), sync_investor_top30),
            ("job_sector_flow", (16, 25), sync_sector_flow)]
    for kind, (h, m), fn in jobs:
        if (now.hour, now.minute) < (h, m) or _brief_already_sent(kind, today):
            continue
        try:
            if _latest_trading_day() != now.strftime("%Y%m%d"):
                return  # 휴장일 또는 아직 KRX 확정 전
            res = fn()
            _mark_brief_sent(kind, today)
            print(f"[sector_jobs] {kind} 완료: {res}")
        except Exception as e:
            print(f"[sector_jobs] {kind} 실패 (5분 뒤 재시도): {e}")


_sector_job_status = {"state": "idle", "result": None}


@app.get("/api/sync-sector-map")
def sync_sector_map_endpoint(dry: bool = True):
    """
    업종 지도 동기화 — dry=true(기본): 업종 3개만 빨리 미리보기(저장 안 함)
    dry=false: 전체(약 80개 업종) 저장을 서버 뒤에서 시작 → /api/sync-sector-status 로 진행 확인
    """
    if dry:
        try:
            return sync_sector_map(dry=True)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def run():
        _sector_job_status.update({"state": "running", "result": None})
        try:
            r1 = sync_sector_map()
            r2 = sync_sector_flow()
            _sector_job_status.update({"state": "done", "result": {"map": r1, "flow": r2}})
        except Exception as e:
            _sector_job_status.update({"state": "error", "result": str(e)})

    threading.Thread(target=run, daemon=True).start()
    return {"ok": True, "started": True, "check": "/api/sync-sector-status"}


@app.get("/api/sync-sector-status")
def sync_sector_status():
    return _sector_job_status


@app.get("/api/sync-investor-top30")
def sync_investor_top30_endpoint():
    try:
        return sync_investor_top30()
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/sync-sector-flow")
def sync_sector_flow_endpoint():
    try:
        return sync_sector_flow()
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# ⚡ 실시간(장중) 수급 — 한투 "국내기관·외국인 매매종목 가집계" (FHPTJ04400000)
# ──────────────────────────────────────────────────────────────────────────
# 장중 증권사들이 집계한 외국인·기관 잠정 순매수 상위/하위 종목 (확정치는 장 마감 후 KRX).
# 📈 선물 투자자별 동향은 한투 "시장별 투자자매매동향(시세)"(FHPTJ04030000)인데 파라미터를 확인해야 해서
#    /api/futures-investor-debug 로 후보를 시험해 보고 맞추는 중.
# ══════════════════════════════════════════════════════════════════════════
_live_cache: dict = {}


def fetch_live_investor_top(market: str = "ALL", who: str = "foreign", side: str = "buy", n: int = 15) -> dict:
    key = (market, who, side)
    c = _live_cache.get(key)
    if c and time.time() - c[0] < 120:
        return c[1]
    iscd = {"ALL": "0000", "KOSPI": "0001", "KOSDAQ": "1001"}.get(market, "0000")
    etc = {"all": "0", "foreign": "1", "institution": "2"}.get(who, "1")
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/foreign-institution-total",
        "FHPTJ04400000",
        {"FID_COND_MRKT_DIV_CODE": "V", "FID_COND_SCR_DIV_CODE": "16449", "FID_INPUT_ISCD": iscd,
         "FID_DIV_CLS_CODE": "1", "FID_RANK_SORT_CLS_CODE": "0" if side == "buy" else "1", "FID_ETC_CLS_CODE": etc},
    )
    rows = []
    for r in (data.get("output") or [])[:n]:
        f = lambda k: float(r.get(k) or 0)
        frgn = f("frgn_ntby_tr_pbmn")
        orgn = f("orgn_ntby_tr_pbmn")
        rows.append({
            "code": r.get("mksc_shrn_iscd"), "name": r.get("hts_kor_isnm"),
            "price": f("stck_prpr"), "pct": f("prdy_ctrt"),
            # 금액은 백만원 단위로 옴 → 억원
            "foreign_eok": round(frgn / 100, 1), "institution_eok": round(orgn / 100, 1),
        })
    out = {"ok": True, "market": market, "who": who, "side": side, "at": now_kst().strftime("%H:%M"), "rows": rows,
           "rt_cd": data.get("rt_cd"), "msg": data.get("msg1")}
    _live_cache[key] = (time.time(), out)
    return out


@app.get("/api/live-investor-top")
def live_investor_top_endpoint(market: str = "ALL", who: str = "foreign", side: str = "buy"):
    """장중 외국인·기관 잠정 순매수(buy)/순매도(sell) 상위 — 2분 캐시"""
    try:
        return fetch_live_investor_top(market, who, side)
    except Exception as e:
        return {"ok": False, "error": str(e)}


_fut_snaps: dict = {"date": None, "rows": []}


def fetch_futures_investor() -> dict:
    """코스피200 선물 투자자별 순매수 (2026-10-14 확인: FID_INPUT_ISCD=K2I, FID_INPUT_ISCD_2=F001, 금액 단위 백만원)"""
    data = kis_get("/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market", "FHPTJ04030000",
                   {"FID_INPUT_ISCD": "K2I", "FID_INPUT_ISCD_2": "F001"})
    o = (data.get("output") or [{}])
    o = o[0] if isinstance(o, list) else o
    f = lambda k: float(o.get(k) or 0)
    return {
        "foreign_qty": f("frgn_ntby_qty"), "foreign_eok": round(f("frgn_ntby_tr_pbmn") / 100, 1),
        "individual_qty": f("prsn_ntby_qty"), "individual_eok": round(f("prsn_ntby_tr_pbmn") / 100, 1),
        "institution_qty": f("orgn_ntby_qty"), "institution_eok": round(f("orgn_ntby_tr_pbmn") / 100, 1),
        "securities_qty": f("scrt_ntby_qty"), "invest_trust_qty": f("ivtr_ntby_qty"),
    }


def _snap_futures():
    """장중 5분마다 선물 투자자별 누적 순매수를 기록 → 시간대별 흐름 차트"""
    now = now_kst()
    if now.weekday() >= 5 or not ((8, 45) <= (now.hour, now.minute) <= (15, 50)):
        return
    today = now.strftime("%Y-%m-%d")
    if _fut_snaps["date"] != today:
        _fut_snaps.update({"date": today, "rows": []})
    try:
        r = fetch_futures_investor()
        r["time"] = now.strftime("%H:%M")
        if not _fut_snaps["rows"] or _fut_snaps["rows"][-1]["time"] != r["time"]:
            _fut_snaps["rows"].append(r)
    except Exception as e:
        print(f"[futures] 기록 실패: {e}")


@app.get("/api/futures-investor")
def futures_investor_endpoint():
    """코스피200 선물 투자자별 순매수 — 지금 값 + 오늘 5분 간격 흐름"""
    try:
        cur = fetch_futures_investor()
        return {"ok": True, "now": now_kst().strftime("%H:%M"), "current": cur, "intraday": _fut_snaps["rows"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


FUT_CANDIDATES = [
    {"FID_INPUT_ISCD": "K2I", "FID_INPUT_ISCD_2": "F001"},
    {"FID_INPUT_ISCD": "999", "FID_INPUT_ISCD_2": "F001"},
    {"FID_INPUT_ISCD": "K2I", "FID_INPUT_ISCD_2": "S001"},
    {"FID_INPUT_ISCD": "0001", "FID_INPUT_ISCD_2": "S001"},
]


@app.get("/api/futures-investor-debug")
def futures_investor_debug():
    """선물 투자자별 동향 — 한투 시장별 투자자매매동향 파라미터 후보를 차례로 시험해서 원본 응답을 보여줌"""
    out = {}
    for i, extra in enumerate(FUT_CANDIDATES):
        try:
            data = kis_get("/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market", "FHPTJ04030000", extra)
            out[f"try{i}:{extra}"] = {"rt_cd": data.get("rt_cd"), "msg": data.get("msg1"),
                                      "preview": json.dumps(data.get("output") or data.get("output1") or data, ensure_ascii=False)[:600]}
        except Exception as e:
            out[f"try{i}:{extra}"] = {"error": str(e)}
        time.sleep(0.3)
    return out


# ══════════════════════════════════════════════════════════════════════════
# 📈 FRED 일별 히스토리 (美 2년물 등 야후에 없는 지표 차트용)
# ══════════════════════════════════════════════════════════════════════════
@app.get("/api/fred-history")
def fred_history_endpoint(series_id: str = "DGS2", weekly: bool = False):
    try:
        obs = list(reversed(_fred_obs(series_id, limit=900)))  # 오래된 순
        pts, seen_week = [], {}
        for d, v in obs:
            dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            t = int(dt.timestamp())
            if weekly:
                seen_week[dt.isocalendar()[:2]] = {"time": t, "open": v, "high": v, "low": v, "close": v}
            else:
                pts.append({"time": t, "open": v, "high": v, "low": v, "close": v})
        if weekly:
            pts = sorted(seen_week.values(), key=lambda p: p["time"])
        return {"ok": True, "series_id": series_id, "points": pts}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 😨 공포지수 — 국내 VKOSPI(코스피200 변동성지수) + 세계 CNN 공포·탐욕 지수
# ══════════════════════════════════════════════════════════════════════════
_fear_cache = {"at": 0.0, "data": None}
_vkospi_ticker = {"code": None}


def _find_vkospi_ticker():
    if _vkospi_ticker["code"]:
        return _vkospi_ticker["code"]
    ymd = _latest_trading_day()
    for mkt in ("KRX", "KOSPI", "테마"):
        try:
            for t in stock.get_index_ticker_list(ymd, market=mkt):
                nm = stock.get_index_ticker_name(t) or ""
                if "변동성" in nm or "VKOSPI" in nm.upper():
                    _vkospi_ticker["code"] = t
                    return t
        except Exception:
            continue
    return None


def _vkospi_from_krx_openapi():
    """KRX Open API 파생상품지수 일별시세(idx/drvprod_dd_trd)에서 '변동성' 지수 — KRX_AUTH_KEY 사용"""
    if not KRX_AUTH_KEY:
        return None
    pts = []
    d = now_kst()
    tries = 0
    while len(pts) < 45 and tries < 75:
        bas = d.strftime("%Y%m%d")
        d -= timedelta(days=1)
        tries += 1
        if datetime.strptime(bas, "%Y%m%d").weekday() >= 5:
            continue
        try:
            res = requests.get("https://data-dbg.krx.co.kr/svc/apis/idx/drvprod_dd_trd",
                               headers={"AUTH_KEY": KRX_AUTH_KEY}, params={"basDd": bas}, timeout=8)
            rows = res.json().get("OutBlock_1", []) if res.ok else []
        except Exception:
            rows = []
        hit = next((r for r in rows if "변동성" in (r.get("IDX_NM") or "")), None)
        if hit and hit.get("CLSPRC_IDX") not in (None, "", "-"):
            pts.append((f"{bas[:4]}-{bas[4:6]}-{bas[6:]}", float(str(hit["CLSPRC_IDX"]).replace(",", ""))))
        elif tries == 1 and not rows and res is not None and not getattr(res, "ok", False):
            break
    return list(reversed(pts)) or None


def fetch_vkospi():
    closes = _vkospi_from_krx_openapi()
    if closes and len(closes) >= 2:
        last, prev = closes[-1][1], closes[-2][1]
        return {"date": closes[-1][0], "value": round(last, 2), "change_pct": round((last / prev - 1) * 100, 2),
                "avg60": round(sum(v for _, v in closes) / len(closes), 2), "history": closes}
    t = _find_vkospi_ticker()
    if not t:
        raise ValueError("VKOSPI를 찾지 못했어요 (KRX Open API '파생상품지수 일별시세' 서비스 신청 필요할 수 있음)")
    end = _latest_trading_day()
    start = (datetime.strptime(end, "%Y%m%d") - timedelta(days=90)).strftime("%Y%m%d")
    df = _krx_df(stock.get_index_ohlcv_by_date, start, end, t)
    closes = [(i.strftime("%Y-%m-%d"), float(r["종가"])) for i, r in df.iterrows()]
    if len(closes) < 2:
        raise ValueError("VKOSPI 데이터가 부족해요")
    last, prev = closes[-1][1], closes[-2][1]
    return {"date": closes[-1][0], "value": round(last, 2), "change_pct": round((last / prev - 1) * 100, 2),
            "avg60": round(sum(v for _, v in closes[-60:]) / len(closes[-60:]), 2), "history": closes[-60:]}


def fetch_cnn_fear_greed():
    res = requests.get(
        "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Referer": "https://edition.cnn.com/markets/fear-and-greed",
            "Origin": "https://edition.cnn.com",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=10,
    )
    res.raise_for_status()
    fg = res.json().get("fear_and_greed") or {}
    kr = {"extreme fear": "극도의 공포", "fear": "공포", "neutral": "중립", "greed": "탐욕", "extreme greed": "극도의 탐욕"}
    return {
        "score": round(float(fg.get("score") or 0), 1),
        "rating": kr.get(str(fg.get("rating") or "").lower(), fg.get("rating")),
        "prev_close": round(float(fg.get("previous_close") or 0), 1),
        "week_ago": round(float(fg.get("previous_1_week") or 0), 1),
        "month_ago": round(float(fg.get("previous_1_month") or 0), 1),
        "updated": str(fg.get("timestamp") or "")[:16],
    }


@app.get("/api/fear-indices")
def fear_indices_endpoint():
    c = _fear_cache
    if c["data"] and time.time() - c["at"] < 30 * 60:
        return c["data"]
    out = {"ok": True}
    for key, fn in (("vkospi", fetch_vkospi), ("cnn", fetch_cnn_fear_greed)):
        try:
            out[key] = fn()
        except Exception as e:
            out[key] = None
            out[f"{key}_error"] = str(e)
    if out.get("vkospi") or out.get("cnn"):
        c.update({"at": time.time(), "data": out})
    return out


# ══════════════════════════════════════════════════════════════════════════
# 📒 회사 소개 (내 관심종목 사전용) — DART 기업개황 + 최근 사업보고서 "사업의 개요"
# ══════════════════════════════════════════════════════════════════════════
_profile_cache: dict = {}


def _extract_business_overview(text: str, length: int = 1200) -> str:
    plain = re.sub(r"<[^>]+>", " ", text)
    plain = re.sub(r"&[a-z]+;", " ", plain)
    plain = re.sub(r"\s+", " ", plain)
    idx = -1
    for key in ("사업의 개요", "사업의개요"):
        # 목차에 먼저 나오는 경우가 많아서 두 번째 등장(본문)을 우선 사용
        first = plain.find(key)
        second = plain.find(key, first + 1) if first >= 0 else -1
        idx = second if second >= 0 else first
        if idx >= 0:
            idx += len(key)
            break
    if idx < 0:
        return ""
    body = plain[idx: idx + length * 2]
    stop = re.search(r"2\.\s*주요\s*제품|2\.\s*주요 제품 및 서비스|주요 제품 및 서비스", body)
    if stop and stop.start() > 200:
        body = body[: stop.start()]
    return body[:length].strip(" .:-")


def fetch_company_profile(code: str) -> dict:
    c = _profile_cache.get(code)
    if c and time.time() - c[0] < 7 * 86400:
        return c[1]
    corp = _dart_corp_code(code)
    if not corp:
        raise ValueError("DART에서 회사를 찾지 못했어요")
    info = requests.get("https://opendart.fss.or.kr/api/company.json",
                        params={"crtfc_key": DART_API_KEY, "corp_code": corp}, timeout=10).json()
    overview, report_nm = "", None
    try:
        lst = requests.get("https://opendart.fss.or.kr/api/list.json", params={
            "crtfc_key": DART_API_KEY, "corp_code": corp, "pblntf_detail_ty": "A001",
            "bgn_de": (now_kst() - timedelta(days=800)).strftime("%Y%m%d"), "page_count": 5,
        }, timeout=10).json()
        reports = lst.get("list") or []
        if reports:
            rcp = reports[0]["rcept_no"]
            report_nm = reports[0].get("report_nm")
            res = requests.get("https://opendart.fss.or.kr/api/document.xml",
                               params={"crtfc_key": DART_API_KEY, "rcept_no": rcp}, timeout=30)
            if res.content[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
                    names = sorted(zf.namelist(), key=lambda n: zf.getinfo(n).file_size, reverse=True)
                    raw = zf.read(names[0])
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    text = raw.decode("euc-kr", errors="replace")
                overview = _extract_business_overview(text)
    except Exception as e:
        print(f"[profile] {code} 사업의 개요 실패: {e}")
    out = {
        "ok": True, "code": code, "name": info.get("corp_name"), "ceo": info.get("ceo_nm"),
        "established": info.get("est_dt"), "homepage": info.get("hm_url"), "address": info.get("adres"),
        "overview": overview, "source": report_nm,
    }
    _profile_cache[code] = (time.time(), out)
    return out


@app.get("/api/company-profile")
def company_profile_endpoint(code: str):
    """회사 소개 — 대표·설립일·홈페이지 + 최근 사업보고서 '사업의 개요' 앞부분 (DART)"""
    try:
        return fetch_company_profile((code or "").strip().upper())
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# ⚡ 종목별 장중 투자자 수급 (추정) — 한투 "종목별 외인기관 추정가집계" (FHPTJ04160001)
# ──────────────────────────────────────────────────────────────────────────
# 증권사들이 장중 5번(09:30·10:00·11:20·13:20·14:30) 집계하는 외국인·기관 추정 순매수(수량).
# 개인은 제공되지 않음(외국인·기관의 반대편으로 추정 가능). 확정치는 장 마감 후 KRX.
# ══════════════════════════════════════════════════════════════════════════
EST_SLOTS = {"1": "09:30", "2": "10:00", "3": "11:20", "4": "13:20", "5": "14:30"}


@app.get("/api/stock-live-flow")
def stock_live_flow_endpoint(code: str):
    code = (code or "").strip().upper()
    try:
        data = kis_get("/uapi/domestic-stock/v1/quotations/investor-trend-estimate", "HHPTJ04160200",
                       {"MKSC_SHRN_ISCD": code})
        rows = data.get("output2") or data.get("output") or []
        if isinstance(rows, dict):
            rows = [rows]
        price = 0.0
        try:
            price = _kis_quote(code)["price"]
        except Exception:
            pass
        slots = []
        for r in rows:
            gb = str(r.get("bsop_hour_gb") or "")
            f = lambda k: float(r.get(k) or 0)
            fq, oq = f("frgn_fake_ntby_qty"), f("orgn_fake_ntby_qty")
            slots.append({
                "slot": EST_SLOTS.get(gb, gb), "foreign_qty": fq, "institution_qty": oq,
                "foreign_eok": round(fq * price / 1e8, 1) if price else None,
                "institution_eok": round(oq * price / 1e8, 1) if price else None,
            })
        slots.sort(key=lambda x: x["slot"])
        return {"ok": True, "code": code, "price": price, "slots": slots,
                "rt_cd": data.get("rt_cd"), "msg": data.get("msg1"),
                "raw_preview": None if slots else json.dumps(data, ensure_ascii=False)[:500]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 📓 매매분석 — 매매한 날의 상황 (가격·투자자별 수급·공시·뉴스)
# ══════════════════════════════════════════════════════════════════════════
_trade_ctx_cache: dict = {}


@app.get("/api/trade-context")
def trade_context_endpoint(code: str, date: str):
    """date=YYYY-MM-DD 그날 이 종목: 시가·고가·저가·종가·등락률·거래대금, 투자자별 순매수(억), 앞뒤 3일 공시, 그날 뉴스"""
    code = (code or "").strip().upper()
    key = (code, date)
    if key in _trade_ctx_cache:
        return _trade_ctx_cache[key]
    ymd = date.replace("-", "")
    out = {"ok": True, "code": code, "date": date}
    # 가격
    try:
        start = (datetime.strptime(ymd, "%Y%m%d") - timedelta(days=10)).strftime("%Y%m%d")
        df = _krx_df(stock.get_market_ohlcv_by_date, start, ymd, code)
        if df is not None and len(df) >= 1:
            r = df.iloc[-1]
            prev = float(df["종가"].iloc[-2]) if len(df) >= 2 else None
            out["price"] = {
                "date": df.index[-1].strftime("%Y-%m-%d"), "open": float(r["시가"]), "high": float(r["고가"]),
                "low": float(r["저가"]), "close": float(r["종가"]), "volume": float(r["거래량"]),
                "pct": round((float(r["종가"]) / prev - 1) * 100, 2) if prev else None,
            }
    except Exception as e:
        out["price_error"] = str(e)
    # 투자자별 순매수 (억원)
    try:
        df = _krx_df(stock.get_market_trading_value_by_date, ymd, ymd, code)
        if df is not None and not df.empty:
            r = df.iloc[-1]
            out["investors"] = {k: round(float(r[k]) / 1e8, 1) for k in df.columns if k in ("개인", "외국인합계", "기관합계", "기타법인")}
    except Exception as e:
        out["investors_error"] = str(e)
    # 공시 (앞뒤 3일)
    if DART_API_KEY:
        try:
            corp = _dart_corp_code(code)
            if corp:
                d0 = datetime.strptime(ymd, "%Y%m%d")
                res = requests.get("https://opendart.fss.or.kr/api/list.json", params={
                    "crtfc_key": DART_API_KEY, "corp_code": corp, "bgn_de": (d0 - timedelta(days=3)).strftime("%Y%m%d"),
                    "end_de": (d0 + timedelta(days=3)).strftime("%Y%m%d"), "page_count": 30}, timeout=10).json()
                out["disclosures"] = [{"date": f"{x['rcept_dt'][:4]}-{x['rcept_dt'][4:6]}-{x['rcept_dt'][6:]}",
                                       "title": re.sub(r"\s+", " ", x.get("report_nm") or "").strip(),
                                       "link": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x['rcept_no']}"}
                                      for x in (res.get("list") or [])]
        except Exception as e:
            out["disclosures_error"] = str(e)
    # 뉴스 (그날 앞뒤 하루, 구글 뉴스)
    try:
        name = _safe_ticker_name(code, allow_remote=False) or code
        d0 = datetime.strptime(ymd, "%Y%m%d")
        q = f'"{name}" after:{(d0 - timedelta(days=1)).strftime("%Y-%m-%d")} before:{(d0 + timedelta(days=2)).strftime("%Y-%m-%d")}'
        res = requests.get("https://news.google.com/rss/search", params={"q": q, "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                           headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        items = []
        for it in ET.fromstring(res.content).iter("item"):
            title = (it.findtext("title") or "").strip()
            src = (it.findtext("source") or "").strip()
            if src and title.endswith(f" - {src}"):
                title = title[: -len(src) - 3]
            items.append({"title": title, "source": src, "link": (it.findtext("link") or "").strip()})
            if len(items) >= 6:
                break
        out["news"] = items
    except Exception as e:
        out["news_error"] = str(e)
    _trade_ctx_cache[key] = out
    return out


# ══════════════════════════════════════════════════════════════════════════
# ⏱ 날짜 지정 1분봉 (단기 매매분석용) — 한투 "주식일별분봉조회" (FHKST03010230)
# 당일·전날 등 과거 날짜의 09:00~15:30 1분봉. 한 번에 120개씩 받아서 앞으로 이어붙임.
# ══════════════════════════════════════════════════════════════════════════
@app.get("/api/stock-minute-by-date")
def stock_minute_by_date_endpoint(code: str, date: str, market: str = "J"):
    code = (code or "").strip().upper()
    ymd = date.replace("-", "")
    market = "UN" if market == "UN" else "J"  # 🔀 UN=🌙 통합 (08:00~20:00)
    first_hm = "090000" if market == "J" else "080000"
    rows, cursor, seen = [], ("153000" if market == "J" else "200000"), set()
    try:
        for _ in range(6 if market == "J" else 16):
            data = kis_get(
                "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
                "FHKST03010230",
                {"FID_COND_MRKT_DIV_CODE": market, "FID_INPUT_ISCD": code, "FID_INPUT_HOUR_1": cursor,
                 "FID_INPUT_DATE_1": ymd, "FID_PW_DATA_INCU_YN": "Y", "FID_FAKE_TICK_INCU_YN": ""},
            )
            batch = [r for r in (data.get("output2") or []) if r.get("stck_bsop_date") == ymd]
            new = [r for r in batch if r.get("stck_cntg_hour") not in seen]
            if not new:
                break
            for r in new:
                seen.add(r["stck_cntg_hour"])
                rows.append(r)
            earliest = min(r["stck_cntg_hour"] for r in new)
            if earliest <= first_hm:
                break
            t = datetime.strptime(earliest, "%H%M%S") - timedelta(minutes=1)
            cursor = t.strftime("%H%M%S")
            time.sleep(0.12)
        candles = sorted(
            [{"time": f"{r['stck_cntg_hour'][:2]}:{r['stck_cntg_hour'][2:4]}", "open": float(r["stck_oprc"]),
              "high": float(r["stck_hgpr"]), "low": float(r["stck_lwpr"]), "close": float(r["stck_prpr"]),
              "volume": float(r.get("cntg_vol") or 0)} for r in rows],
            key=lambda c: c["time"],
        )
        return {"ok": bool(candles), "code": code, "date": date, "candles": candles,
                "error": None if candles else "그날 분봉이 없어요 (휴장일이거나 너무 오래된 날짜)"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 🌐 해외 종목 검색 (전 세계, 야후) — 한글 이름(대형주 약 190개)·영문 이름·티커
# ══════════════════════════════════════════════════════════════════════════
GLOBAL_KR_EXTRA = {
    "도쿄일렉트론": "8035.T", "어드반테스트": "6857.T", "소프트뱅크그룹": "9984.T", "패스트리테일링": "9983.T",
    "키옥시아": "285A.T", "무라타": "6981.T", "도요타": "7203.T", "소니": "6758.T", "닌텐도": "7974.T",
    "구이저우마오타이": "600519.SS", "마오타이": "600519.SS", "CATL": "300750.SZ", "텐센트": "0700.HK",
    "알리바바": "9988.HK", "샤오미": "1810.HK", "BYD": "1211.HK", "TSMC": "2330.TW", "폭스콘": "2317.TW",
    "SAP": "SAP.DE", "지멘스": "SIE.DE", "LVMH": "MC.PA", "에르메스": "RMS.PA", "토탈에너지스": "TTE.PA",
    "노보노디스크": "NOVO-B.CO", "셸": "SHEL.L", "아스트라제네카": "AZN.L",
}


@app.get("/api/global-search")
def global_search_endpoint(q: str):
    q = (q or "").strip()
    if not q:
        return {"ok": True, "results": []}
    out, seen = [], set()

    def add(sym, name, exch=""):
        if sym and sym not in seen:
            seen.add(sym)
            out.append({"symbol": sym, "name": name, "exchange": exch})

    # 1) 한글 이름
    for kr, sym in GLOBAL_KR_EXTRA.items():
        if q in kr:
            add(sym, kr)
    for sym, kr in US_KR_NAMES.items():
        if q in kr:
            add(sym, kr, "미국")
    # 2) 야후 검색 (영문 이름·티커)
    try:
        res = requests.get("https://query1.finance.yahoo.com/v1/finance/search",
                           params={"q": q, "quotesCount": 10, "newsCount": 0},
                           headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        for it in (res.json().get("quotes") or []):
            if it.get("quoteType") in ("EQUITY", "ETF"):
                add(it.get("symbol"), US_KR_NAMES.get(it.get("symbol")) or it.get("shortname") or it.get("longname") or it.get("symbol"),
                    it.get("exchDisp") or "")
    except Exception as e:
        print(f"[global-search] 야후 검색 실패: {e}")
    return {"ok": True, "results": out[:15]}


@app.get("/api/global-ohlcv")
def global_ohlcv_endpoint(symbol: str, start: str, end: str = ""):
    """해외 종목 일봉 — 시작~종료일을 정확히 (야후 period1/period2). start/end: YYYY-MM-DD"""
    try:
        p1 = int(datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        e = datetime.strptime(end, "%Y-%m-%d") if end else datetime.utcnow()
        p2 = int((e + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp())
        res = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(symbol)}",
                           params={"period1": p1, "period2": p2, "interval": "1d", "events": "split"},
                           headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r = res.json()["chart"]["result"][0]
        q = r["indicators"]["quote"][0]
        meta = r.get("meta", {})
        out = []
        for i, t in enumerate(r.get("timestamp") or []):
            o, h, l, c, v = (q.get(k, [None])[i] for k in ("open", "high", "low", "close", "volume"))
            if None in (o, h, l, c):
                continue
            out.append({"time": int(t), "date": datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d"),
                        "open": o, "high": h, "low": l, "close": c, "volume": v or 0})
        kr = US_KR_NAMES.get(symbol.upper()) or next((k for k, v in GLOBAL_KR_EXTRA.items() if v == symbol.upper()), None)
        return {"ok": bool(out), "symbol": symbol, "currency": meta.get("currency"), "candles": out,
                "name": kr or meta.get("shortName") or meta.get("longName") or symbol,
                "error": None if out else "데이터가 없어요"}
    except Exception as ex:
        return {"ok": False, "error": str(ex)}


# ══════════════════════════════════════════════════════════════════════════
# 🇺🇸 해외 시장분석·랭킹 — 나스닥 공식 스크리너(미국 전 종목: 현재가·등락률·시총·섹터·거래량)
# ──────────────────────────────────────────────────────────────────────────
# 미국은 투자자별 수급이 공개되지 않아서, "돈이 몰리는 곳"을 거래대금·섹터 ETF 자금 흐름으로 대신 봄.
# 한 번 받으면 5분 보관 (전 종목 약 7천 개).
# ══════════════════════════════════════════════════════════════════════════
_us_screen_cache = {"at": 0.0, "rows": []}
SECTOR_KR = {
    "Technology": "기술", "Health Care": "헬스케어", "Finance": "금융", "Consumer Discretionary": "경기소비재",
    "Consumer Staples": "필수소비재", "Industrials": "산업재", "Energy": "에너지", "Utilities": "유틸리티",
    "Real Estate": "부동산", "Telecommunications": "통신", "Basic Materials": "소재", "Miscellaneous": "기타",
}


def _num_us(v):
    try:
        return float(str(v).replace("$", "").replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def fetch_us_screener() -> list:
    c = _us_screen_cache
    if c["rows"] and time.time() - c["at"] < 300:
        return c["rows"]
    rows = []
    for ex in ("nasdaq", "nyse", "amex"):
        try:
            res = requests.get("https://api.nasdaq.com/api/screener/stocks",
                               params={"tableonly": "true", "limit": 10000, "exchange": ex, "download": "true"},
                               headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}, timeout=25)
            data = (res.json() or {}).get("data") or {}
            for r in data.get("rows") or []:
                price, pct, cap, vol = _num_us(r.get("lastsale")), _num_us(r.get("pctchange")), _num_us(r.get("marketCap")), _num_us(r.get("volume"))
                sym = (r.get("symbol") or "").strip()
                if not sym or price is None or "^" in sym or "/" in sym:
                    continue
                raw_name = r.get("name") or sym
                # 워런트·유닛·권리·우선주는 랭킹에서 제외 (2026-10-18 CORZW 같은 워런트가 섞였음)
                if re.search(r"\b(Warrants?|Units?|Rights?|Preferred|Depositary Shares? representing)\b", raw_name, re.I):
                    continue
                rows.append({
                    "symbol": sym,
                    "name": _us_name(sym, re.sub(r"\s*(Common (Stock|Shares?)|Ordinary Shares?|American Depositary Shares?|Class [A-C].*|Capital Stock)\s*", " ", raw_name).strip())[:30],
                    "exchange": ex.upper(), "price": price, "pct": pct or 0.0, "cap": cap or 0.0, "volume": vol or 0.0,
                    "value": round(price * (vol or 0)), "sector": SECTOR_KR.get(r.get("sector") or "", r.get("sector") or "기타"),
                    "industry": r.get("industry") or "",
                })
        except Exception as e:
            print(f"[us-screener] {ex} 실패: {e}")
    if rows:
        c.update({"at": time.time(), "rows": rows})
    return rows


@app.get("/api/us-market-map")
def us_market_map_endpoint(top: int = 300):
    """미국 시장 지도 — 시가총액 상위 N개를 섹터별로 (등락률·시총·거래대금)"""
    rows = sorted([r for r in fetch_us_screener() if r["cap"] > 0], key=lambda r: -r["cap"])[: max(50, min(top, 600))]
    sectors = {}
    for r in rows:
        sectors.setdefault(r["sector"], []).append(r)
    out = []
    for sec, items in sectors.items():
        cap = sum(i["cap"] for i in items)
        out.append({"sector": sec, "cap": cap, "pct": round(sum(i["pct"] * i["cap"] for i in items) / cap, 2) if cap else 0,
                    "value": sum(i["value"] for i in items), "stocks": items})
    return {"ok": bool(out), "sectors": sorted(out, key=lambda x: -x["cap"]), "count": len(rows)}


@app.get("/api/us-ranking")
def us_ranking_endpoint(sort: str = "value", min_cap_b: float = 1.0, min_price: float = 1.0, limit: int = 30, sector: str = ""):
    """미국 랭킹 — sort: value(거래대금)/gainers/losers/cap/volume · 시총 하한(억달러 아닌 10억달러 B 단위)·가격 하한"""
    rows = [r for r in fetch_us_screener() if r["cap"] >= min_cap_b * 1e9 and r["price"] >= min_price and (not sector or r["sector"] == sector)]
    key = {"value": lambda r: -r["value"], "gainers": lambda r: -r["pct"], "losers": lambda r: r["pct"],
           "cap": lambda r: -r["cap"], "volume": lambda r: -r["volume"]}.get(sort, lambda r: -r["value"])
    return {"ok": True, "sort": sort, "rows": sorted(rows, key=key)[: max(10, min(limit, 100))], "total": len(rows)}


# 섹터 ETF 자금 흐름 — 오늘 거래대금이 최근 20일 평균보다 얼마나 많은지 + 등락 (돈이 어느 섹터로 가나)
SECTOR_ETFS = [
    ("XLK", "기술"), ("SMH", "반도체"), ("XLC", "통신서비스"), ("XLY", "경기소비재"), ("XLF", "금융"),
    ("XLV", "헬스케어"), ("XLI", "산업재"), ("XLE", "에너지"), ("XLB", "소재"), ("XLP", "필수소비재"),
    ("XLU", "유틸리티"), ("XLRE", "부동산"), ("ARKK", "혁신성장"), ("IWM", "중소형주"),
]
_etf_flow_cache = {"at": 0.0, "data": None}


@app.get("/api/us-sector-flow")
def us_sector_flow_endpoint():
    c = _etf_flow_cache
    if c["data"] and time.time() - c["at"] < 600:
        return c["data"]
    out = []
    for sym, kr in SECTOR_ETFS:
        try:
            res = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}", params={"range": "1mo", "interval": "1d"},
                               headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
            r = res.json()["chart"]["result"][0]
            q = r["indicators"]["quote"][0]
            pts = [(cl, v) for cl, v in zip(q.get("close") or [], q.get("volume") or []) if cl is not None and v is not None]
            if len(pts) < 3:
                continue
            vals = [cl * v for cl, v in pts]
            today_v, avg20 = vals[-1], sum(vals[:-1]) / max(1, len(vals) - 1)
            out.append({"symbol": sym, "name": kr, "price": round(pts[-1][0], 2), "pct": round((pts[-1][0] / pts[-2][0] - 1) * 100, 2),
                        "value": round(today_v), "ratio": round(today_v / avg20, 2) if avg20 else None,
                        "pct5": round((pts[-1][0] / pts[-6][0] - 1) * 100, 2) if len(pts) >= 6 else None})
        except Exception as e:
            print(f"[sector-flow] {sym} 실패: {e}")
        time.sleep(0.1)
    data = {"ok": bool(out), "rows": sorted(out, key=lambda x: -(x["pct"] or 0))}
    if out:
        c.update({"at": time.time(), "data": data})
    return data


# ══════════════════════════════════════════════════════════════════════════
# 🔎 해외(미국) 종목 상세 — SEC 공식 공시·재무제표 + 나스닥 애널리스트 목표가·회사 소개 + 뉴스
# ──────────────────────────────────────────────────────────────────────────
# SEC(미국 증권거래위원회)는 User-Agent에 연락처를 넣어야 응답함. 공시·재무는 무료 공식 데이터.
# ══════════════════════════════════════════════════════════════════════════
SEC_HEADERS = {"User-Agent": os.environ.get("SEC_USER_AGENT", "JM Tracker jm-tracker@example.com"), "Accept-Encoding": "gzip, deflate"}
_sec_cik = {"at": 0.0, "map": {}}
_us_detail_cache: dict = {}


def _sec_cik_of(symbol: str):
    c = _sec_cik
    if not c["map"] or time.time() - c["at"] > 7 * 86400:
        res = requests.get("https://www.sec.gov/files/company_tickers.json", headers=SEC_HEADERS, timeout=20)
        c["map"] = {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in res.json().values()}
        c["at"] = time.time()
    return c["map"].get(symbol.upper().replace(".", "-"))


def _sec_quarterly(facts: dict):
    gaap = (facts.get("facts") or {}).get("us-gaap") or {}

    def series(names):
        for n in names:
            units = (gaap.get(n) or {}).get("units") or {}
            vals = units.get("USD") or []
            q = {}
            for v in vals:
                fr = v.get("frame") or ""
                if re.fullmatch(r"CY\d{4}Q\d", fr):
                    q[fr] = v.get("val")
            if q:
                return q
        return {}

    rev = series(["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"])
    op = series(["OperatingIncomeLoss"])
    net = series(["NetIncomeLoss"])
    frames = sorted(set(rev) | set(op) | set(net))[-8:]
    return [{"period": f"{f[2:6]}Q{f[-1]}", "revenue": rev.get(f), "op": op.get(f), "net": net.get(f)} for f in frames]


@app.get("/api/us-stock-detail")
def us_stock_detail_endpoint(symbol: str):
    symbol = (symbol or "").strip().upper()
    c = _us_detail_cache.get(symbol)
    if c and time.time() - c[0] < 1800:
        return c[1]
    out = {"ok": True, "symbol": symbol}
    # SEC 공시 + 재무
    try:
        cik = _sec_cik_of(symbol)
        out["cik"] = cik
        if cik:
            sub = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=SEC_HEADERS, timeout=15).json()
            rec = (sub.get("filings") or {}).get("recent") or {}
            filings = []
            for i, form in enumerate(rec.get("form") or []):
                if form not in ("10-K", "10-Q", "8-K", "20-F", "6-K", "S-1", "DEF 14A", "4", "13D", "SC 13G"):
                    continue
                acc = rec["accessionNumber"][i].replace("-", "")
                filings.append({"date": rec["filingDate"][i], "form": form, "desc": (rec.get("primaryDocDescription") or [""] * 999)[i] or "",
                                "link": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{rec['primaryDocument'][i]}"})
                if len(filings) >= 30:
                    break
            out["filings"] = filings
            out["company"] = {"name": sub.get("name"), "industry": sub.get("sicDescription"), "exchange": ", ".join(sub.get("exchanges") or [])}
            facts = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json", headers=SEC_HEADERS, timeout=20).json()
            out["quarters"] = _sec_quarterly(facts)
    except Exception as e:
        out["sec_error"] = str(e)
    # 나스닥: 애널리스트 목표가·회사 소개
    nh = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        t = requests.get(f"https://api.nasdaq.com/api/analyst/{symbol}/targetprice", headers=nh, timeout=10).json()
        co = ((t.get("data") or {}).get("consensusOverview")) or {}
        out["target"] = {k: co.get(k) for k in ("lowPriceTarget", "highPriceTarget", "priceTarget", "buy", "sell", "hold")}
    except Exception as e:
        out["target_error"] = str(e)
    try:
        prof = requests.get(f"https://api.nasdaq.com/api/company/{symbol}/company-profile", headers=nh, timeout=10).json()
        d = (prof.get("data") or {})
        out["profile"] = {"description": ((d.get("CompanyDescription") or {}).get("value")), "sector": ((d.get("Sector") or {}).get("value")),
                          "industry": ((d.get("Industry") or {}).get("value")), "website": ((d.get("CompanyUrl") or {}).get("value"))}
    except Exception as e:
        out["profile_error"] = str(e)
    # 뉴스 (한국어 기사 위주)
    try:
        kr = US_KR_NAMES.get(symbol) or next((k for k, v in GLOBAL_KR_EXTRA.items() if v == symbol), None)
        q = f"{kr or symbol} 주가" + (f" OR {symbol}" if kr else "")
        res = requests.get("https://news.google.com/rss/search", params={"q": f"{q} when:7d", "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                           headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        items = []
        for it in ET.fromstring(res.content).iter("item"):
            title = (it.findtext("title") or "").strip()
            src = (it.findtext("source") or "").strip()
            if src and title.endswith(f" - {src}"):
                title = title[: -len(src) - 3]
            items.append({"title": title, "source": src, "link": (it.findtext("link") or "").strip(), "date": (it.findtext("pubDate") or "")[:16]})
            if len(items) >= 20:
                break
        out["news"] = items
    except Exception as e:
        out["news_error"] = str(e)
    _us_detail_cache[symbol] = (time.time(), out)
    return out


@app.get("/api/global-minute")
def global_minute_endpoint(symbol: str, interval: str = "1m"):
    """해외 종목 분봉 (야후, 최근 7일 1분봉 / 60일 5분봉) — 현지 시각으로 날짜별 묶음"""
    try:
        rng = "7d" if interval == "1m" else "60d"
        res = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(symbol)}",
                           params={"range": rng, "interval": interval}, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r = res.json()["chart"]["result"][0]
        off = (r.get("meta") or {}).get("gmtoffset") or 0
        q = r["indicators"]["quote"][0]
        days = {}
        for i, t in enumerate(r.get("timestamp") or []):
            o, h, l, c, v = (q.get(k, [None])[i] for k in ("open", "high", "low", "close", "volume"))
            if None in (o, h, l, c):
                continue
            local = datetime.fromtimestamp(t + off, tz=timezone.utc)
            days.setdefault(local.strftime("%Y-%m-%d"), []).append(
                {"time": local.strftime("%H:%M"), "ts": int(t), "open": o, "high": h, "low": l, "close": c, "volume": v or 0})
        return {"ok": bool(days), "symbol": symbol, "currency": (r.get("meta") or {}).get("currency"), "gmtoffset": off, "gmtoffset": off,
                "days": [{"date": d, "candles": days[d]} for d in sorted(days)], "error": None if days else "분봉 데이터가 없어요"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 🇺🇸 미국장 텔레그램 알림 — 국내 하루 흐름(장전 → 개장 초반 → 마감)과 같은 리듬
#   · 개장 1시간 전 (여름 21:30 / 겨울 22:30 KST): 📋 미국 장전 리포트
#   · 개장 30분 후 (여름 23:00 / 겨울 24:00 KST): 🚀 미국 개장 초반 상승 랭킹
#   · 06:15 KST: ☀️ 미국 마감 (기존 + 급등락·섹터·한국 관련주·해외 관심종목)
# 서머타임은 _us_to_kst가 자동 반영. 미국 휴장일은 나스닥 market-info로 확인.
# ══════════════════════════════════════════════════════════════════════════
_us_bday_cache = {"date": None, "val": True}

# 미국 대장주 → 한국 관련주 (움직임이 크면 같이 알려줌)
KR_LINKS = {
    "NVDA": "SK하이닉스·한미반도체·삼성전자", "AMD": "삼성전자·SK하이닉스", "MU": "SK하이닉스·삼성전자", "AVGO": "SK하이닉스",
    "TSM": "삼성전자·DB하이텍", "^SOX": "반도체 전반", "TSLA": "LG에너지솔루션·에코프로비엠·삼성SDI", "AAPL": "LG이노텍·비에이치",
    "META": "SK하이닉스(HBM)", "MSFT": "SK하이닉스·삼성전자(데이터센터)", "GOOGL": "SK하이닉스", "AMZN": "SK하이닉스",
    "LLY": "HLB·알테오젠(바이오)", "NVO": "펩트론·한미약품(비만약)", "BA": "한화에어로스페이스·KAI", "XOM": "S-Oil·SK이노베이션",
}


def _us_business_day() -> bool:
    today = now_kst().strftime("%Y-%m-%d")
    c = _us_bday_cache
    if c["date"] == today:
        return c["val"]
    val = now_kst().weekday() < 5
    try:
        j = requests.get("https://api.nasdaq.com/api/market-info", headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}, timeout=8).json()
        d = j.get("data") or {}
        if "isBusinessDay" in d:
            val = bool(d["isBusinessDay"])
    except Exception:
        pass
    c.update({"date": today, "val": val})
    return val


def _us_session_date():
    """지금 걸쳐 있는 미국 세션 날짜 — 한국 새벽(0~6시)은 전날 밤에 시작한 세션 (겨울 개장 30분 후 = 00:00 대비)"""
    return (now_kst() - timedelta(hours=6)).strftime("%Y-%m-%d")


def _us_open_kst():
    """이번 미국 정규장 개장 시각(KST) — 서머타임 자동 반영 (여름 22:30 / 겨울 23:30)"""
    return _us_to_kst(_us_session_date(), "09:30")


def _q_line(sym, label):
    q = _yahoo_quote(sym)
    if not q:
        return None
    return f"{'🔴▲' if q['pct'] > 0 else '🔵▼' if q['pct'] < 0 else '⚪'} {label} {q['price']:,.2f} ({q['pct']:+.2f}%)"


def _global_watch_rows():
    try:
        return supabase.table("global_watchlist").select("symbol,name,target_price,stop_price").execute().data or []
    except Exception:
        return []


def build_us_pre_brief():
    now = now_kst()
    lines = [f"📋 미국 장전 리포트 ({now.month}/{now.day} 밤 · 개장 {_us_open_kst().strftime('%H:%M')} KST)\n━━━━━━━━━━━━"]
    fut = [l for l in (_q_line("NQ=F", "나스닥100 선물"), _q_line("ES=F", "S&P500 선물"), _q_line("YM=F", "다우 선물")) if l]
    if fut:
        lines.append("📈 미국 선물\n" + "\n".join(fut))
    mac = [l for l in (_q_line("^VIX", "VIX"), _q_line("^TNX", "미 10년물"), _q_line("DX-Y.NYB", "달러인덱스"), _q_line("CL=F", "WTI"), _q_line("BTC-USD", "비트코인")) if l]
    if mac:
        lines.append("🌐 금리·달러·원자재\n" + "\n".join(mac))
    eu = [l for l in (_q_line("^GDAXI", "독일 DAX"), _q_line("^FCHI", "프랑스 CAC"), _q_line("^STOXX50E", "유로스톡스50")) if l]
    if eu:
        lines.append("🇪🇺 유럽 장중\n" + "\n".join(eu))
    try:
        cal = fetch_calendar(2)
        tonight = []
        for e in cal["events"]:
            if not e["kind"].startswith("us_"):
                continue
            if e["date"] == now.strftime("%Y-%m-%d") or (e["date"] == (now + timedelta(days=1)).strftime("%Y-%m-%d") and (e.get("time") or "99") < "07:00"):
                tonight.append(_event_line(e).split("\n")[0])
        if tonight:
            lines.append("🗓 오늘 밤 미국 일정\n" + "\n".join(tonight[:10]))
    except Exception as e:
        print(f"[us_pre] 일정 실패: {e}")
    wl = _global_watch_rows()
    if wl:
        wq = []
        for w in wl[:15]:
            q = _yahoo_quote(w["symbol"])
            if q:
                wq.append(f"· {w.get('name') or w['symbol']} {q['price']:,.2f} (전일 {q['pct']:+.2f}%)")
        if wq:
            lines.append("⭐ 해외 관심종목\n" + "\n".join(wq))
    return "\n\n".join(lines), now.strftime("%Y-%m-%d")


def build_us_open30_brief(top_n: int = 20):
    now = now_kst()
    rows = [r for r in fetch_us_screener() if r["cap"] >= 2e9 and r["price"] >= 5]
    if not rows:
        return None, None
    # 같은 회사의 다른 주식 종류(예: Zillow Z·ZG)는 하나만
    seen, gain = set(), []
    for r in sorted(rows, key=lambda r: -r["pct"]):
        base = re.sub(r"[^a-z]", "", r["name"].lower())[:12]
        if base in seen:
            continue
        seen.add(base)
        gain.append(r)
        if len(gain) >= top_n:
            break
    lines = [f"🚀 미국 개장 초반 상승 랭킹 ({now.strftime('%H:%M')} KST · 개장 30분)\n━━━━━━━━━━━━", "시가총액 20억달러↑ · 주가 $5↑"]
    for i, r in enumerate(gain, 1):
        kr = KR_LINKS.get(r["symbol"])
        lines.append(f"{i}. {r['name']} ({r['symbol']}) {r['pct']:+.2f}% · ${r['price']:,.2f} · 거래대금 ${r['value'] / 1e6:,.0f}M"
                     + (f"\n   🇰🇷 관련: {kr}" if kr else "") + f"\n   🔗 {FRONTEND_URL}/global-analysis?symbol={r['symbol']}")
    val = sorted(rows, key=lambda r: -r["value"])[:5]
    lines.append("\n💵 거래대금 TOP5: " + ", ".join(f"{r['name']} {r['pct']:+.1f}%" for r in val))
    return "\n".join(lines), now.strftime("%Y-%m-%d")


def _us_close_extras() -> str:
    parts = []
    rows = [r for r in fetch_us_screener() if r["cap"] >= 10e9]
    if rows:
        g = sorted(rows, key=lambda r: -r["pct"])[:5]
        l = sorted(rows, key=lambda r: r["pct"])[:5]
        parts.append("🔺 대형주 상승 TOP5: " + ", ".join(f"{r['name']} {r['pct']:+.1f}%" for r in g))
        hot = sorted([r for r in fetch_us_screener() if r["cap"] >= 2e9 and r["value"] >= 2e8 and abs(r["pct"]) >= 7],
                     key=lambda r: -abs(r["pct"]))[:5]
        if hot:
            parts.append("🔥 해외 특징주 (시총 20억$↑·거래대금 2억$↑·±7%↑)\n" + "\n".join(
                f"· {r['name']} ({r['symbol']}) {r['pct']:+.1f}%" + (f" → 🇰🇷 {KR_LINKS[r['symbol']]}" if r['symbol'] in KR_LINKS else "") for r in hot))
        parts.append("🔻 대형주 하락 TOP5: " + ", ".join(f"{r['name']} {r['pct']:+.1f}%" for r in l))
        links = []
        by = {r["symbol"]: r for r in rows}
        for sym, kr in KR_LINKS.items():
            r = by.get(sym)
            if r and abs(r["pct"]) >= 2:
                links.append(f"· {r['name']} {r['pct']:+.1f}% → {kr}")
        # 🇰🇷 한국 관련주 체크는 뺌 (2026-11-07 JM)
    try:
        fl = us_sector_flow_endpoint().get("rows") or []
        if fl:
            parts.append("💵 섹터: 강세 " + ", ".join(f"{f['name']} {f['pct']:+.1f}%" for f in fl[:3]) + " / 약세 " + ", ".join(f"{f['name']} {f['pct']:+.1f}%" for f in fl[-3:]))
    except Exception:
        pass
    wl = []  # ⭐ 해외 관심종목은 뺌 — 새로 만들 예정 (2026-11-07 JM)
    hits = []
    for w in wl[:20]:
        q = _yahoo_quote(w["symbol"])
        if not q:
            continue
        nm = w.get("name") or w["symbol"]
        if w.get("target_price") and q["price"] >= float(w["target_price"]):
            hits.append(f"🎯 {nm} 목표가 도달 ({q['price']:,.2f} ≥ {float(w['target_price']):,.2f})")
        elif w.get("stop_price") and q["price"] <= float(w["stop_price"]):
            hits.append(f"🛑 {nm} 손절가 이탈 ({q['price']:,.2f} ≤ {float(w['stop_price']):,.2f})")
        elif abs(q["pct"]) >= 3:
            hits.append(f"⚡ {nm} {q['pct']:+.1f}%")
    if hits:
        parts.append("⭐ 해외 관심종목\n" + "\n".join(hits))
    return "\n\n".join(parts)


def _run_us_briefs():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    now = now_kst()
    sess = _us_session_date()
    if datetime.strptime(sess, "%Y-%m-%d").weekday() >= 5:
        return
    try:
        open_kst = _us_open_kst().replace(tzinfo=None)
    except Exception:
        return
    nowl = now.replace(tzinfo=None)
    today = sess
    for kind, at, builder in (
        ("us_pre", open_kst - timedelta(minutes=65), build_us_pre_brief),  # 🇺🇸 22:25 (겨울) — 2026-11-07
        ("us_open30", open_kst + timedelta(minutes=30), build_us_open30_brief),
    ):
        if not (at <= nowl <= at + timedelta(minutes=40)) or _brief_already_sent(kind, today):
            continue
        if not _us_business_day():
            continue
        try:
            text, ref = builder()
            if text and send_brief(kind, text):
                _mark_brief_sent(kind, today)
                print(f"[us_briefs] {kind} 발송")
        except Exception as e:
            print(f"[us_briefs] {kind} 실패: {e}")


# ══════════════════════════════════════════════════════════════════════════
# 📒 해외 종목 사전 자동 생성 — 국내 사전(키워드 + 날짜별 이슈)과 같은 모양
# ──────────────────────────────────────────────────────────────────────────
#   · 대상: 미국 시가총액 상위 N개 + 아시아·유럽 대표 종목
#   · 키워드: 나스닥 섹터·산업 → 한국어 테마 + 대표 테마(AI·원전·우주·양자·비만약…) + 🇰🇷 관련주
#   · 이슈: 최근 30일 한국어 뉴스 제목(날짜별, 종목당 최대 5개) — 매주 토요일 새 이슈만 추가
#   · 내가 직접 추가한 이슈는 지우지 않고 합쳐서 보관
# ══════════════════════════════════════════════════════════════════════════
INDUSTRY_KR = [
    ("Semiconductor", "반도체"), ("Prepackaged Software", "소프트웨어"), ("Programming", "소프트웨어"), ("EDP Services", "IT서비스"),
    ("Internet", "인터넷·플랫폼"), ("Computer Communications", "네트워크장비"), ("Computer Manufacturing", "컴퓨터·하드웨어"),
    ("Biotechnology", "바이오"), ("Pharmaceutical", "제약"), ("Medical", "의료기기"), ("Hospital", "헬스케어서비스"),
    ("Major Banks", "은행"), ("Banks", "은행"), ("Investment Bankers", "증권"), ("Finance", "금융"), ("Insurance", "보험"),
    ("Auto Manufacturing", "자동차"), ("Auto Parts", "자동차부품"), ("Electric Utilities", "전력"), ("Power Generation", "전력"),
    ("Oil", "석유·가스"), ("Natural Gas", "석유·가스"), ("Aerospace", "항공우주"), ("Military", "방산"), ("Real Estate Investment", "리츠"),
    ("Industrial Machinery", "기계"), ("Electrical Products", "전기장비"), ("Restaurants", "외식"), ("Retail", "유통"),
    ("Department", "유통"), ("Telecommunications Equipment", "통신장비"), ("Telecom", "통신"), ("Broadcasting", "미디어"),
    ("Movies", "엔터·미디어"), ("Railroads", "운송"), ("Air Freight", "운송"), ("Trucking", "운송"), ("Airlines", "항공"),
    ("Hotels", "여행·레저"), ("Beverages", "음료"), ("Packaged Foods", "식품"), ("Apparel", "의류"), ("Steel", "철강"),
    ("Metal", "금속·광물"), ("Mining", "금속·광물"), ("Chemicals", "화학"), ("Construction", "건설"), ("Homebuilding", "건설"),
    ("Consumer Electronics", "전자제품"), ("Industrial Specialties", "산업재"), ("Specialty Insurers", "보험"),
]
THEME_RULES = {
    "AI": ["NVDA", "AMD", "AVGO", "MSFT", "GOOGL", "GOOG", "META", "PLTR", "ARM", "SMCI", "ORCL", "CRM", "NOW", "SNOW", "AI", "SOUN", "TSM", "ASML"],
    "데이터센터": ["VRT", "ETN", "ANET", "DELL", "SMCI", "EQIX", "DLR", "CEG", "VST", "NVDA", "CRWV", "NBIS", "APLD", "IREN"],
    "HBM·메모리": ["MU", "SNDK", "WDC", "STX"],
    "원전·SMR": ["CEG", "VST", "OKLO", "SMR", "CCJ", "BWXT", "LEU", "NNE", "TLN"],
    "우주": ["RKLB", "ASTS", "LUNR", "PL", "SPCE", "RDW", "BA", "LMT"],
    "양자컴퓨터": ["IONQ", "RGTI", "QBTS", "QUBT", "QNT", "IBM", "GOOGL"],
    "가상자산": ["COIN", "MSTR", "MARA", "RIOT", "CRCL", "HOOD", "CLSK", "WULF", "CORZ", "GLXY"],
    "비만약": ["LLY", "NVO", "VKTX", "AMGN"],
    "로봇": ["TSLA", "ISRG", "SYM", "TER", "ROK", "SERV"],
    "전기차·배터리": ["TSLA", "RIVN", "LCID", "ALB", "QS", "ENVX"],
    "방산": ["LMT", "NOC", "RTX", "GD", "LHX", "KTOS", "AVAV", "PLTR", "HII"],
    "사이버보안": ["CRWD", "PANW", "ZS", "FTNT", "NET", "OKTA", "S"],
    "스테이블코인·핀테크": ["CRCL", "PYPL", "SQ", "XYZ", "SOFI", "AFRM", "V", "MA"],
}


def _themes_for(sym: str):
    return [t for t, syms in THEME_RULES.items() if sym in syms]


def _news_issues(query: str, days: int = 30, limit: int = 5):
    from email.utils import parsedate_to_datetime
    res = requests.get("https://news.google.com/rss/search", params={"q": f"{query} when:{days}d", "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                       headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    out, seen = [], set()
    for it in ET.fromstring(res.content).iter("item"):
        title = (it.findtext("title") or "").strip()
        src = (it.findtext("source") or "").strip()
        if src and title.endswith(f" - {src}"):
            title = title[: -len(src) - 3]
        key = re.sub(r"[^가-힣A-Za-z0-9]", "", title)[:20]
        if not title or key in seen:
            continue
        seen.add(key)
        try:
            d = parsedate_to_datetime(it.findtext("pubDate")).astimezone(KST).strftime("%Y-%m-%d")
        except Exception:
            continue
        out.append({"date": d, "text": f"{title} ({src})" if src else title})
        if len(out) >= limit:
            break
    return out


# 원어 뉴스 — 시장별 언어 (미국 영어 / 일본 일본어 / 중국·홍콩 중국어)
NEWS_LANG = {
    "en": {"hl": "en-US", "gl": "US", "ceid": "US:en"},
    "ja": {"hl": "ja", "gl": "JP", "ceid": "JP:ja"},
    "zh": {"hl": "zh-TW", "gl": "HK", "ceid": "HK:zh-Hant"},
}


def _lang_of(sym: str) -> str:
    if sym.endswith(".T"):
        return "ja"
    if sym.endswith((".HK", ".SS", ".SZ", ".TW")):
        return "zh"
    return "en"


def _news_original(sym: str, name: str, days: int = 30, limit: int = 5):
    from email.utils import parsedate_to_datetime
    lang = _lang_of(sym)
    code = sym.split(".")[0]
    q = {"en": f'"{name}" stock OR "{code}" shares', "ja": f'"{code}" 株', "zh": f'"{code.lstrip("0") or code}" 股價 OR "{name}"'}[lang]
    res = requests.get("https://news.google.com/rss/search", params={"q": f"{q} when:{days}d", **NEWS_LANG[lang]},
                       headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    out, seen = [], set()
    for it in ET.fromstring(res.content).iter("item"):
        title = (it.findtext("title") or "").strip()
        src = (it.findtext("source") or "").strip()
        if src and title.endswith(f" - {src}"):
            title = title[: -len(src) - 3]
        key = title[:30].lower()
        if not title or key in seen:
            continue
        seen.add(key)
        try:
            d = parsedate_to_datetime(it.findtext("pubDate")).astimezone(KST).strftime("%Y-%m-%d")
        except Exception:
            continue
        out.append({"date": d, "text": title, "source": src, "link": (it.findtext("link") or "").strip()})
        if len(out) >= limit:
            break
    return out, lang


def _asia_universe(per_group: int = 200):
    """한투 해외 업종(일본·홍콩/중국)에서 업종별 종목을 모아 야후 티커로 — (티커, 이름, 업종)"""
    out = []
    for group in ("JP", "HK_CN"):
        try:
            sectors = fetch_overseas_sector_list(group, limit=40)
        except Exception as e:
            print(f"[global_dict] {group} 업종 목록 실패: {e}")
            continue
        rows = []
        for sec in sectors:
            try:
                for r in fetch_overseas_sector_detail(group, sec["icod"], limit=30):
                    rows.append((r, sec["sector_name"]))
            except Exception:
                pass
            time.sleep(0.15)
        rows.sort(key=lambda x: -(x[0].get("volume") or 0))
        seen = set()
        for r, secname in rows:
            m = (r.get("market") or "").upper()
            code = str(r.get("symbol") or "").strip()
            if m.startswith("TSE"):
                tk = f"{code}.T"
            elif m.startswith("HKS"):
                tk = f"{str(int(code)) if code.isdigit() else code}".zfill(4) + ".HK"
            elif m.startswith("SHS"):
                tk = f"{code}.SS"
            elif m.startswith("SZS"):
                tk = f"{code}.SZ"
            else:
                continue
            if tk in seen:
                continue
            seen.add(tk)
            out.append((tk, r.get("stock_name") or tk, secname))
            if len([1 for o in out if o[0].endswith(".T")]) >= per_group and group == "JP":
                break
            if len([1 for o in out if not o[0].endswith(".T")]) >= per_group and group == "HK_CN":
                break
    return out


EXTRA_GLOBAL = [(v, k) for k, v in GLOBAL_KR_EXTRA.items()]
_gdict_status = {"state": "idle", "done": 0, "total": 0, "error": None}


def build_global_dictionary(top: int = 600, asia: int = 200):
    st = _gdict_status
    st.update({"state": "running", "done": 0, "error": None})
    try:
        rows = sorted([r for r in fetch_us_screener() if r["cap"] > 0], key=lambda r: -r["cap"])[:top]
        items = [(r["symbol"], r["name"], r.get("sector") or "", r.get("industry") or "") for r in rows]
        have = {r[0] for r in items}
        for tk, nm, secname in _asia_universe(asia):  # 🇯🇵 일본·🇭🇰 홍콩·🇨🇳 중국 (업종 이름 포함)
            if tk not in have:
                have.add(tk)
                items.append((tk, nm, secname, ""))
        for sym, kr in EXTRA_GLOBAL:
            if sym not in have:  # "마오타이·구이저우마오타이"처럼 같은 티커가 두 번 있으면 한 번만 (2026-10-18 오류)
                have.add(sym)
                items.append((sym, kr, "", ""))
        st["total"] = len(items)
        existing = {}
        try:
            for i in range(0, 4000, 1000):
                d = supabase.table("stock_dictionary").select("code,issues").eq("market", "GLOBAL").range(i, i + 999).execute().data or []
                existing.update({x["code"]: x.get("issues") or [] for x in d})
                if len(d) < 1000:
                    break
        except Exception:
            pass
        batch = []
        for sym, name, sector, industry in items:
            ind_kr = next((kr for key, kr in INDUSTRY_KR if key.lower() in industry.lower()), None)
            themes = _themes_for(sym)
            tags = [t for t in [sector or None, ind_kr] if t] + [t for t in themes if t not in (sector, ind_kr)]
            kr_rel = KR_LINKS.get(sym)
            subtags = [x.strip() for x in kr_rel.split("·")] if kr_rel else []
            kw = ",".join(tags) + (f" / 🇰🇷 {kr_rel}" if kr_rel else "")
            krname = US_KR_NAMES.get(sym) or next((k for k, v in GLOBAL_KR_EXTRA.items() if v == sym), None)
            try:
                news = _news_issues(f"\"{krname}\"" if krname else f"\"{name}\" 주가")
            except Exception:
                news = []
            try:
                orig, lang = _news_original(sym, name)
            except Exception:
                orig, lang = [], _lang_of(sym)
            prev = existing.get(sym, [])
            have_keys = {(i.get("date"), (i.get("text") or "")[:30]) for i in prev}
            merged = prev + [n for n in news if (n["date"], n["text"][:30]) not in have_keys]
            merged.sort(key=lambda i: i.get("date") or "", reverse=True)
            batch.append({
                "code": sym, "name": krname or name, "market": "GLOBAL", "keywords_short": kw, "keywords_full": kw,
                "tags": tags, "subtags": subtags, "issues": merged[:30],
                "issues_original": orig, "lang": lang,
                "last_issue_date": max([x["date"] for x in merged[:1] + orig[:1]] or [None]) if (merged or orig) else None,
                "updated_at": now_kst().isoformat(),
            })
            st["done"] += 1
            if len(batch) >= 50:
                batch = list({b["code"]: b for b in batch}.values())
                supabase.table("stock_dictionary").upsert(batch, on_conflict="code").execute()
                batch = []
            time.sleep(0.4)
        if batch:
            batch = list({b["code"]: b for b in batch}.values())
            supabase.table("stock_dictionary").upsert(batch, on_conflict="code").execute()
        st["state"] = "done"
    except Exception as e:
        st.update({"state": "error", "error": str(e)})


@app.get("/api/build-global-dictionary")
def build_global_dictionary_endpoint(top: int = 600):
    """해외 종목 사전 만들기(서버 뒤에서, 미국 600 + 일본·홍콩·중국 약 400 → 15~20분) — 진행: /api/build-global-dictionary-status"""
    if _gdict_status["state"] == "running":
        return {"ok": True, "running": True, **_gdict_status}
    threading.Thread(target=build_global_dictionary, args=(max(50, min(top, 1000)),), daemon=True).start()
    return {"ok": True, "started": True, "check": "/api/build-global-dictionary-status"}


@app.get("/api/build-global-dictionary-status")
def build_global_dictionary_status():
    return _gdict_status


def _weekly_global_dictionary():
    """매주 토요일 10시 이후 1번 — 해외 사전 새 이슈 추가"""
    now = now_kst()
    if now.weekday() != 5 or now.hour < 10:
        return
    key = now.strftime("%Y-%m-%d")
    if _brief_already_sent("job_global_dict", key) or _gdict_status["state"] == "running":
        return
    _mark_brief_sent("job_global_dict", key)
    threading.Thread(target=build_global_dictionary, daemon=True).start()


# ══════════════════════════════════════════════════════════════════════════
# 🔔 정비 알림 — 키 연장·일정 갱신을 잊지 않게 텔레그램으로 (해마다 반복)
#   · 6/25, 7/1  : KRX Open API·한국투자증권 앱키 갱신 (7월 말 발급 → 7월 초 연장)
#   · 1/2        : FOMC·금통위 새해 일정 갱신
# ══════════════════════════════════════════════════════════════════════════
MAINTENANCE_REMINDERS = [
    ((6, 25), "🔑 [정비] API 키 갱신 준비 (다음 주부터 7월 초 연장 신청)\n"
              "· KRX Open API 인증키 — openapi.krx.co.kr → 마이페이지 → 인증키 신청내역 → 연장\n"
              "· 한국투자증권 앱키 — KIS Developers → API 신청 내역 → 기간 연장\n"
              "· 새 키를 다시 발급받으면 Render 환경변수(KRX_AUTH_KEY / KIS_APP_KEY / KIS_APP_SECRET)도 교체"),
    ((7, 1), "🔑 [정비] 오늘부터 API 키 연장 신청하세요 (7월 말 만료 예정)\n"
             "· KRX Open API 인증키 (선물 시세·VKOSPI)\n"
             "· 한국투자증권 앱키 (실시간 체결·수급·랭킹 — 핵심)\n"
             "· KRX 데이터 비밀번호를 바꿨다면 Render의 KRX_PW도 변경"),
    ((1, 2), "📅 [정비] 새해 일정 갱신 — 올해 FOMC·금통위 날짜를 Claude에게 알려주고 서버에 반영하세요 "
             "(main.py의 FOMC_DECISION_DATES / BOK_MPC_DATES / BOJ_DECISION_DATES / ECB_DECISION_DATES / KR_GDP_DATES / MSCI_REVIEW_DATES)"),
]


def _maintenance_reminders():
    now = now_kst()
    if now.hour < 9 or now.weekday() >= 5:
        return  # 주말이면 월요일에 몰아서
    days = [now] + ([now - timedelta(days=1), now - timedelta(days=2)] if now.weekday() == 0 else [])
    for (m, d), text in MAINTENANCE_REMINDERS:
        hit = next((x for x in days if (x.month, x.day) == (m, d)), None)
        if hit:
            now = hit
            key = f"maint-{m:02d}{d:02d}-{now.year}"
            if not _brief_already_sent("maintenance", key) and send_telegram(text):
                _mark_brief_sent("maintenance", key)


# ══════════════════════════════════════════════════════════════════════════
# 📰 주요뉴스 (원어) — 한국·미국·러시아·일본·중국·중동(사우디 아랍어) · 분야별
# ──────────────────────────────────────────────────────────────────────────
# 구글 뉴스 국가판 RSS (무료, 키 불필요) — 각 나라 언어 그대로. 10분 보관.
# 텔레그램: 07:10 출근길 · 18:10 퇴근길 "🌍 세계 주요뉴스(원어)" — 나라별 상위 3개
# ══════════════════════════════════════════════════════════════════════════
NEWS_COUNTRIES = {
    "kr": {"name": "🇰🇷 한국", "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
    "us": {"name": "🇺🇸 미국", "hl": "en-US", "gl": "US", "ceid": "US:en"},
    "ru": {"name": "🇷🇺 러시아", "hl": "ru", "gl": "RU", "ceid": "RU:ru"},
    "jp": {"name": "🇯🇵 일본", "hl": "ja", "gl": "JP", "ceid": "JP:ja"},
    "cn": {"name": "🇨🇳 중국", "hl": "zh-CN", "gl": "CN", "ceid": "CN:zh-Hans"},
    "me": {"name": "🇸🇦 중동(아랍)", "hl": "ar", "gl": "SA", "ceid": "SA:ar"},
    "es": {"name": "🇪🇸 스페인어권", "hl": "es", "gl": "ES", "ceid": "ES:es"},
}
NEWS_TOPICS = {"top": None, "world": "WORLD", "nation": "NATION", "business": "BUSINESS", "technology": "TECHNOLOGY",
               "science": "SCIENCE", "health": "HEALTH", "sports": "SPORTS", "entertainment": "ENTERTAINMENT"}
_world_news_cache: dict = {}
_ko_cache: dict = {}


_gtx_block = {"until": 0.0}
_LANG_MYMEMORY = {"en": "en", "ru": "ru", "ja": "ja", "zh": "zh-CN", "zh-CN": "zh-CN", "ar": "ar", "es": "es", "en-US": "en"}


def _gtx_one_batch(texts: list):
    """구글 무료 번역 한 번 호출 — 막혔으면(429·이상한 응답) 30분 쉼"""
    if time.time() < _gtx_block["until"]:
        return None
    try:
        res = requests.get("https://translate.googleapis.com/translate_a/single",
                           params={"client": "gtx", "sl": "auto", "tl": "ko", "dt": "t", "q": "\n".join(texts)},
                           headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        if res.status_code != 200:
            _gtx_block["until"] = time.time() + 1800
            print(f"[translate] 구글 번역 응답 {res.status_code} → 30분 쉬고 대체 번역 사용")
            return None
        joined = "".join(seg[0] for seg in (res.json()[0] or []) if seg and seg[0])
        return [x.strip() for x in joined.split("\n")]
    except Exception as e:
        print(f"[translate] 구글 번역 실패: {e}")
        return None


def _mymemory_one(text: str, src: str):
    """대체 번역 (MyMemory 무료) — 원어 언어를 알아야 함"""
    sl = _LANG_MYMEMORY.get(src or "")
    if not sl:
        return None
    try:
        j = requests.get("https://api.mymemory.translated.net/get", params={"q": text[:480], "langpair": f"{sl}|ko"}, timeout=10).json()
        t = ((j or {}).get("responseData") or {}).get("translatedText")
        return t.strip() if t and "MYMEMORY WARNING" not in t.upper() else None
    except Exception:
        return None


def translate_to_ko(texts: list, src: str = None) -> list:
    """원어 제목 → 한국어. ① 구글 무료(20개씩 묶음) → 줄 수가 안 맞으면 한 줄씩 → ② 구글이 막히면 MyMemory.
    한 번 번역한 제목은 기억해서 다시 안 물어봄 (2026-10-23: 번역이 통째로 사라지던 문제 수정)"""
    out = [None] * len(texts)
    for i, t in enumerate(texts):
        if t in _ko_cache:
            out[i] = _ko_cache[t]
    todo = [(i, t) for i, t in enumerate(texts) if t and out[i] is None]
    for k in range(0, len(todo), 20):
        chunk = todo[k:k + 20]
        parts = _gtx_one_batch([t.replace("\n", " ") for _, t in chunk])
        if parts and len(parts) == len(chunk):
            for (i, t), ko in zip(chunk, parts):
                _ko_cache[t] = ko
                out[i] = ko
            continue
        for i, t in chunk:  # 묶음이 어긋나거나 막혔으면 한 줄씩
            one = _gtx_one_batch([t.replace("\n", " ")])
            ko = one[0] if one else _mymemory_one(t, src)
            if ko:
                _ko_cache[t] = ko
                out[i] = ko
            time.sleep(0.15)
        time.sleep(0.2)
    if len(_ko_cache) > 20000:
        _ko_cache.clear()
    return out


def fetch_world_news(country: str = "us", topic: str = "top", limit: int = 30):
    from email.utils import parsedate_to_datetime
    c = NEWS_COUNTRIES.get(country)
    if not c:
        return []
    key = (country, topic)
    hit = _world_news_cache.get(key)
    if hit and time.time() - hit[0] < 600:
        return hit[1][:limit]
    sec = NEWS_TOPICS.get(topic)
    url = f"https://news.google.com/rss/headlines/section/topic/{sec}" if sec else "https://news.google.com/rss"
    res = requests.get(url, params={"hl": c["hl"], "gl": c["gl"], "ceid": c["ceid"]}, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
    items, seen = [], set()
    for it in ET.fromstring(res.content).iter("item"):
        title = (it.findtext("title") or "").strip()
        src = (it.findtext("source") or "").strip()
        if src and title.endswith(f" - {src}"):
            title = title[: -len(src) - 3]
        k = title[:40]
        if not title or k in seen:
            continue
        seen.add(k)
        try:
            dt = parsedate_to_datetime(it.findtext("pubDate")).astimezone(KST)
            at = dt.strftime("%m-%d %H:%M")
        except Exception:
            at = ""
        items.append({"title": title, "source": src, "link": (it.findtext("link") or "").strip(), "at": at})
    if country != "kr" and items:
        kos = translate_to_ko([n["title"] for n in items[:60]], src=c["hl"].split("-")[0] if c["hl"] != "zh-CN" else "zh-CN")
        for n, ko in zip(items, kos):
            n["ko"] = ko
    _world_news_cache[key] = (time.time(), items)
    return items[:limit]


@app.get("/api/world-news")
def world_news_endpoint(country: str = "us", topic: str = "top", limit: int = 30):
    try:
        return {"ok": True, "country": country, "topic": topic, "name": NEWS_COUNTRIES.get(country, {}).get("name"),
                "items": fetch_world_news(country, topic, max(5, min(limit, 60)))}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def build_world_news_digest():
    now = now_kst()
    lines = [f"🌍 세계 주요뉴스 원어 ({now.strftime('%m/%d %H:%M')})\n━━━━━━━━━━━━"]
    for code, c in NEWS_COUNTRIES.items():
        try:
            items = fetch_world_news(code, "top", 3)
        except Exception:
            items = []
        if not items:
            continue
        lines.append(f"\n{c['name']}")
        for n in items:
            lines.append(f"· {n['title']} ({n['source']})" + (f"\n   ↳ {n['ko']}" if n.get("ko") else ""))
    lines.append(f"\n🔗 전체 보기: {FRONTEND_URL}/world-news")
    return "\n".join(lines), now.strftime("%Y-%m-%d %H")


def _run_world_news_alerts():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    now = now_kst()
    if now.weekday() >= 5:
        return  # 주말 알림 없음
    for kind, (h, m) in (("worldnews_am", (7, 10)), ("worldnews_pm", (18, 10))):
        at = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if not (at <= now <= at + timedelta(minutes=40)):
            continue
        key = now.strftime("%Y-%m-%d")
        if _brief_already_sent(kind, key):
            continue
        try:
            text, _ = build_world_news_digest()
            if send_telegram(text, *bot("news")):
                _mark_brief_sent(kind, key)
        except Exception as e:
            print(f"[worldnews] {kind} 실패: {e}")


# ══════════════════════════════════════════════════════════════════════════
# 🚨 속보 알림 — 국내 증시 이슈 / 해외 증시 이슈 (단타·특징주용)
# ──────────────────────────────────────────────────────────────────────────
#  [국내] ① DART 실시간 주요 공시 (정정공시 제외 = 신규만): 공급계약(금액·매출 대비 %)·유무상증자·자사주·
#          최대주주 변경·합병분할·CB/BW·감자·횡령배임·영업정지·주요경영사항(임상·특허)·기술이전·신규시설투자
#         ② 뉴스 키워드 (최근 1시간): 수주·공급계약·FDA·품목허가·임상·기술이전·인수 — 종목명이 제목에 있을 때만
#  [해외] 공급망·빅테크 사건 (최근 1시간): TSMC·마이크론·엔비디아·삼성·하이닉스·ASML·애플·테슬라… ×
#         파업·화재·정전·지진·가동중단·수출규제·관세·리콜 → 한국어 번역 + 🇰🇷 관련주
#  · 5분마다 · 같은 공시/기사는 한 번만 · 00:00~06:30에 잡힌 건 모아서 06:30 이후 한 번에
# ══════════════════════════════════════════════════════════════════════════
FLASH_DART_RULES = [
    (r"단일판매.*공급계약|공급계약체결", "📝 신규 공급계약", "contract"),
    (r"무상증자결정", "🎁 무상증자", ""), (r"유상증자결정", "⚠️ 유상증자(희석)", ""),
    (r"자기주식.*(취득|소각)|자기주식취득", "💰 자사주 취득·소각", ""),
    (r"^최대주주\s*변경", "👤 최대주주 변경", ""), (r"합병결정|분할결정|주식교환|영업양수|영업양도", "🔀 합병·분할·양수도", ""),
    (r"(전환사채|신주인수권부사채|교환사채)권?\s*발행결정", "⚠️ CB·BW 발행(희석)", ""), (r"감자결정", "⚠️ 감자", ""),
    (r"횡령|배임", "🚫 횡령·배임", ""), (r"영업정지|생산중단", "🚫 영업·생산 중단", ""),
    (r"기술이전|라이선스", "🧬 기술이전·라이선스", ""), (r"투자판단\s*관련\s*주요경영사항", "🧪 주요경영사항(임상·특허 등)", ""),
    (r"신규시설투자", "🏭 신규 시설투자", ""), (r"^주권매매거래정지|상장적격성\s*실질심사\s*대상\s*결정|관리종목\s*지정", "🚫 거래정지·적격성", ""),
]
FLASH_KR_NEWS_QUERIES = ['"수주"', '"공급계약"', '"FDA 승인"', '"품목허가"', '"임상 성공"', '"톱라인"', '"기술이전"', '"기술수출"', '"인수"', '"단독"']
# 제목에 이 말이 꼭 있어야 보냄 (구글 검색이 느슨해서 키워드 없는 기사도 섞였음 — 2026-10-23)
FLASH_KR_MUST = re.compile(r"수주|공급계약|계약\s*체결|FDA\s*(승인|허가)|품목허가|임상.{0,6}(성공|승인|유효|달성)|톱라인|기술이전|기술수출|인수|합병|M&A|최대\s*실적|단독")
# 기획·인터뷰·칼럼은 매매와 상관없어서 제외
FLASH_KR_SKIP = re.compile(r"인터뷰|칼럼|기고|사설|기획|커버스토리|COVER STORY|비즈리더|돋보기|창립|\d+년\s*-|100년|주간|리뷰|전망|분석|\?|…까|할까|될까|볼까|"
                           r"박차|추진|노린다|목표|참가|참석|전시회|박람회|기대감|"
                           r"증권사\s*리포트|리포트|목표가|투자의견|브리핑|요약|모닝|마감시황|시황|특징주\s*모음")  # 리포트 요약·시황 정리는 속보가 아님
# 속보가 아닌 출처 (유료 뉴스레터·리포트 요약·블로그형 콘텐츠·공시 재가공 분석 기사)
FLASH_KR_SKIP_SRC = re.compile(r"프리미엄콘텐츠|프리미엄 콘텐츠|블로그|카페|포스트|유튜브|YouTube|브런치|데일리연합")
# 분쟁·공방·해설 기사 ("재인수"처럼 키워드만 겹치는 경영권 분쟁 뉴스 등)
FLASH_KR_SKIP2 = re.compile(r"분쟁|공방|주장|의혹|소송|고발|반박|드러난|계보|짚어|따져|보도…|논란|우려|감소·|MBK")
FLASH_KR_TAG = re.compile(r"^\[(?!단독|속보)[^\]]{1,30}\]")
# 바이오 뉴스는 너무 잦아서 "최초"가 제목에 있을 때만 (국내 최초·세계 최초·글로벌 최초 등) — 2026-10-23 요청
FLASH_KR_BIO = re.compile(r"임상|FDA|품목허가|신약|바이오|항암|치료제|백신|톱라인|기술이전|기술수출|라이선스|제약|의약|파이프라인|희귀질환|세포|유전자|항체")


def _bio_without_first(title: str) -> bool:
    return bool(FLASH_KR_BIO.search(title)) and "최초" not in title
FLASH_GLOBAL_COMPANIES = {
    "TSMC": "삼성전자·DB하이텍", "Micron": "SK하이닉스·삼성전자", "Nvidia": "SK하이닉스·한미반도체", "Samsung": "삼성전자",
    "SK hynix": "SK하이닉스", "ASML": "반도체 장비주", "Apple": "LG이노텍·비에이치", "Tesla": "LG에너지솔루션·에코프로비엠",
    "Intel": "반도체", "AMD": "삼성전자·SK하이닉스", "Broadcom": "SK하이닉스", "Foxconn": "전자부품주", "Qualcomm": "삼성전자",
}
FLASH_GLOBAL_EVENTS_EN = ['"workers strike"', '"on strike"', '"to strike"', '"strike vote"', '"labor strike"', '"plant fire"', '"factory fire"', '"fab fire"',
                          'outage', 'earthquake', '"production halt"', '"export controls"', 'recall', 'explosion']
FLASH_GLOBAL_EVENTS_KO = ["파업", "화재", "정전", "지진", "가동중단", "수출규제", "관세", "리콜", "폭발"]
_flash_state = {"at": 0.0, "names": None, "names_at": 0.0, "pending": [], "waiting": {}}
# 🌐 해외 증시 속보 끄기 (2026-10-26 JM: 리포트 쓰는 데 쓸모가 적음) — 다시 켜려면 True
#   꺼두면 06:15 아침 알림의 "🌙 밤사이 해외 속보"도 같이 안 나옴. 국내 속보는 그대로
FLASH_GLOBAL_ENABLED = False
# 회사 이름에 사건 단어가 들어 있거나 다른 뜻인 경우 (예: 삼성화재 = Samsung Fire, 뉴욕 별명 Big Apple)
FLASH_GLOBAL_FALSE = re.compile(r"samsung fire|fire\s*&\s*marine|big apple|apple(bee|ton)|pineapple|apple tv|ordered to pay|lawsuit|verdict|"
                                r"strikes? (a )?deal|strike(s)? .{0,20}(deal|partnership|agreement)|tv deals?|black friday|lit a|ends .{0,10}strike", re.I)
FLASH_EVENT_WORDS = {"workers strike": "파업", "on strike": "파업", "strike vote": "파업", "labor strike": "파업", "to strike": "파업",
                     "plant fire": "화재", "factory fire": "화재", "fab fire": "화재", "outage": "정전", "earthquake": "지진", "production halt": "가동중단",
                     "export controls": "수출규제", "tariff": "관세", "recall": "리콜", "explosion": "폭발",
                     "파업": "파업", "화재": "화재", "정전": "정전", "지진": "지진", "가동중단": "가동중단", "수출규제": "수출규제",
                     "관세": "관세", "리콜": "리콜", "폭발": "폭발"}


_flash_mem: set = set()  # 서버 메모리에도 기억 — DB 조회가 실패해도 같은 속보를 다시 안 보냄 (2026-10-22 중복 수정)


def _flash_seen(key: str) -> bool:
    if key in _flash_mem:
        return True
    try:
        res = supabase.table("telegram_sent_log").select("kind").eq("kind", "flash").eq("ref_date", key).limit(1).execute()
        if res.data:
            _flash_mem.add(key)
            return True
        return False
    except Exception as e:
        print(f"[flash] 보낸 기록 조회 실패 → 안전하게 '보냄'으로 처리: {e}")
        return True  # 확인이 안 되면 보내지 않는 쪽으로 (중복보다 누락이 낫다)


def _flash_mark(key: str):
    _flash_mem.add(key)
    _mark_brief_sent("flash", key)


def _flash_stock_names():
    st = _flash_state
    if st["names"] and time.time() - st["names_at"] < 6 * 3600:
        return st["names"]
    names = {}
    try:
        for i in range(0, 4000, 1000):
            d = supabase.table("sector_stocks").select("code,name").range(i, i + 999).execute().data or []
            for r in d:
                if r.get("name") and len(r["name"]) >= 3:  # 2글자 이름은 오탐이 많아 제외
                    names[r["name"]] = r["code"]
            if len(d) < 1000:
                break
    except Exception:
        pass
    st.update({"names": names, "names_at": time.time()})
    return names


def _contract_details(rcept_no: str) -> str:
    """공급계약 공시 원문에서 계약금액·매출액 대비(%)·상대방 꺼내기"""
    try:
        res = requests.get("https://opendart.fss.or.kr/api/document.xml", params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}, timeout=15)
        if res.content[:2] != b"PK":
            return ""
        with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
            raw = b"".join(zf.read(n) for n in zf.namelist())
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("euc-kr", errors="replace")
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            plain = re.sub(r"\s+", " ", BeautifulSoup(text, "html.parser").get_text(" "))
        amt = re.search(r"계약금액[^0-9]{0,30}([\d,]{5,})", plain)
        ratio = re.search(r"매출액\s*대비\s*\(?%?\)?\s*([\d.]+)", plain)
        party = re.search(r"계약상대(?:방)?\s*([^\s]{2,30})", plain)
        parts = []
        if amt:
            won = float(amt.group(1).replace(",", ""))
            parts.append(f"계약금액 {won / 1e8:,.0f}억" if won >= 1e8 else f"계약금액 {amt.group(1)}")
        if ratio:
            parts.append(f"매출 대비 {ratio.group(1)}%")
        if party:
            parts.append(f"상대방 {party.group(1)}")
        return " · ".join(parts)
    except Exception as e:
        print(f"[flash] 계약 원문 실패 {rcept_no}: {e}")
        return ""


_flash_q_cache: dict = {}
# 속보 거래대금 기준 (억원) — 장 초반 50억 터진 종목이 이후에도 강했던 경험 기준 (2026-10-23 JM). 아침 상승 랭킹도 50억 기준
FLASH_MIN_VALUE_EOK = 50


def _flash_liquid(code: str, min_eok: float) -> bool:
    """장중엔 거래대금이 너무 작은 종목은 속보에서 뺌 (단타 대상이 아님). 장 전·주말엔 거르지 않음"""
    now = now_kst()
    if now.weekday() >= 5 or (now.hour, now.minute) < (9, 10):
        return True
    try:
        q = _flash_q_cache.get(code)
        if not q or time.time() - q[0] > 300:
            q = (time.time(), _kis_quote(code))
            _flash_q_cache[code] = q
        return (q[1].get("value_eok") or 0) >= min_eok
    except Exception:
        return True


def _after_close() -> bool:
    """정규장 마감(15:30) 이후 ~ 다음 날 장 시작 전 — 이때는 진짜 큰 호재·악재만 보냄"""
    now = now_kst()
    return (now.hour, now.minute) >= (15, 30) or (now.hour, now.minute) < (8, 30)


def _big_amount_eok(text: str) -> float:
    """제목·계약 내용에서 금액(억) 꺼내기: '2조원'·'1,200억' 등"""
    best = 0.0
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*조", text):
        best = max(best, float(m.group(1)) * 10000)
    for m in re.finditer(r"([\d,]{3,})\s*억", text):
        best = max(best, float(m.group(1).replace(",", "")))
    return best


def _is_big_dart(label: str, nm: str, extra: str) -> bool:
    """장 마감 뒤에도 보낼 만한 공시인가 — 대형 호재·악재만"""
    if label.startswith(("🚫", "⚠️", "👤", "🔀", "🎁")):  # 거래정지·횡령·희석·최대주주 변경·합병분할·무상증자
        return True
    if label.startswith("💰"):
        return "소각" in nm  # 자사주는 소각만
    if label.startswith("📝"):
        ratio = re.search(r"매출 대비 ([\d.]+)%", extra or "")
        return (ratio and float(ratio.group(1)) >= 20) or _big_amount_eok(extra or "") >= 1000
    return False  # 시설투자·주요경영사항·기술이전 등은 장중에만


def _is_big_news(title: str) -> bool:
    """장 마감 뒤 뉴스: '단독' + 1,000억 이상, 또는 바이오 '최초'"""
    return ("단독" in title and _big_amount_eok(title) >= 1000) or (bool(FLASH_KR_BIO.search(title)) and "최초" in title)


def _flash_wait(key: str, code: str, head: str, body: str, skey: str = None):
    """재료는 나왔는데 아직 거래대금이 50억 전인 종목 — 오늘 장 끝날 때까지 지켜보다가 터지면 보냄"""
    _flash_state["waiting"][key] = {"code": code, "head": head, "body": body, "skey": skey,
                                    "day": now_kst().strftime("%Y-%m-%d"), "at": now_kst().strftime("%H:%M")}


def _check_flash_waiting() -> list:
    """대기 목록 다시 확인 → 거래대금 50억 넘고 상승 중이면 '⚡ 거래대금 터짐'으로 보냄"""
    now = now_kst()
    today = now.strftime("%Y-%m-%d")
    out = []
    for key, w in list(_flash_state["waiting"].items()):
        if w["day"] != today:
            _flash_state["waiting"].pop(key, None)
            continue
        if _flash_seen(key):
            _flash_state["waiting"].pop(key, None)
            continue
        try:
            q = _kis_quote(w["code"])
            _flash_q_cache[w["code"]] = (time.time(), q)
        except Exception:
            continue
        if (q.get("value_eok") or 0) >= FLASH_MIN_VALUE_EOK and (q.get("pct") or 0) > 0:
            out.append({"key": key, "skey": w.get("skey"), "cat": "kr",
                        "text": f"⚡ 재료 뒤 거래대금 터짐 (재료 {w['at']} → 지금 {q['value_eok']:,.0f}억)\n{w['head']}{_flash_quote_txt(w['code'])}\n{w['body']}"})
            _flash_state["waiting"].pop(key, None)
        time.sleep(0.1)
    return out


def _flash_quote_txt(code: str) -> str:
    now = now_kst()
    if now.weekday() >= 5 or (now.hour, now.minute) < (9, 0):
        return " (장 시작 전)"  # 장 전엔 시세가 0으로 나와서 헷갈렸음
    try:
        q = _kis_quote(code)
        if not q.get("value_eok"):
            return " (장 시작 전)"
        return f" ({'🔴' if q['pct'] > 0 else '🔵'}{q['pct']:+.2f}% · {q['price']:,.0f}원 · 거래대금 {q['value_eok']:,.0f}억)"
    except Exception:
        return ""


def _collect_flash_domestic() -> list:
    out = []
    now = now_kst()
    # ① DART 실시간 주요 공시 (오늘, 정정 제외)
    if DART_API_KEY:
        try:
            rows = _dart_list(now.strftime("%Y%m%d"), now.strftime("%Y%m%d"), max_pages=2)
        except Exception as e:
            rows = []
            print(f"[flash] DART 목록 실패: {e}")
        for r in rows:
            nm = r.get("report_nm") or ""
            if "정정" in nm or "기타시장안내" in nm or "기한 안내" in nm or not r.get("stock_code"):
                continue
            if re.search(r"종속회사|자회사", nm):  # 자회사 공시는 본업 영향이 작아서 제외 (한솔홀딩스 사례)
                continue
            rule = next((x for x in FLASH_DART_RULES if re.search(x[0], nm)), None)
            if not rule:
                continue
            key = f"dart:{r['rcept_no']}"
            if _flash_seen(key):
                continue
            if key in _flash_state["waiting"]:
                continue
            extra = _contract_details(r["rcept_no"]) if rule[2] == "contract" else ""
            nm_clean = re.sub(r"\s+", " ", nm).strip()  # (f-string 안 백슬래시는 Python 3.11 이하에서 오류라 밖으로 뺌)
            if _after_close() and not _is_big_dart(rule[1], nm, extra):
                _flash_mark(key)  # 장 마감 뒤 작은 공시는 보내지 않음
                continue
            head = f"{rule[1]} · {r.get('corp_name')}"
            body = (f"   {nm_clean}" + (f"\n   {extra}" if extra else "")
                    + f"\n   📎 공시 원문: https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r['rcept_no']}"
                    + f"\n   🔗 {FRONTEND_URL}/stock-analysis?code={r['stock_code']}")
            if not rule[1].startswith(("🚫", "⚠️")) and not _flash_liquid(r["stock_code"], FLASH_MIN_VALUE_EOK):
                _flash_wait(key, r["stock_code"], head, body)  # 거래대금 50억 전 → 대기 목록에 두고, 나중에 터지면 보냄
                continue
            out.append({"key": key, "text": f"{head}{_flash_quote_txt(r['stock_code'])}\n{body}", "cat": "kr"})
    # ② 뉴스 키워드 (최근 1시간, 종목명이 제목에 있을 때만)
    names = _flash_stock_names()
    for q in FLASH_KR_NEWS_QUERIES:
        try:
            res = requests.get("https://news.google.com/rss/search", params={"q": f"{q} when:1h", "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                               headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            for it in ET.fromstring(res.content).iter("item"):
                title = (it.findtext("title") or "").strip()
                src = (it.findtext("source") or "").strip()
                link = (it.findtext("link") or "").strip()
                if src and title.endswith(f" - {src}"):
                    title = title[: -len(src) - 3]
                if not FLASH_KR_MUST.search(title) or FLASH_KR_SKIP.search(title) or FLASH_KR_TAG.search(title) or _bio_without_first(title) \
                        or FLASH_KR_SKIP_SRC.search(src) or FLASH_KR_SKIP2.search(title):
                    continue
                hit = max((n for n in names if n in title), key=len, default=None)
                if not hit:
                    continue
                key = "krnews:" + re.sub(r"[^가-힣A-Za-z0-9]", "", title)[:40]
                if _flash_seen(key) or any(o["key"] == key for o in out):
                    continue
                code = names[hit]
                if _after_close() and not _is_big_news(title):
                    _flash_mark(key)  # 장 마감 뒤엔 대형 뉴스만
                    continue
                skey = f"krstock:{code}:{now.strftime('%Y-%m-%d')}"  # 같은 종목 뉴스는 하루 한 번만
                if _flash_seen(skey) or any(o.get("skey") == skey for o in out) or key in _flash_state["waiting"]:
                    _flash_mark(key)
                    continue
                if not _flash_liquid(code, FLASH_MIN_VALUE_EOK):  # 거래대금 50억 전 → 대기, 나중에 터지면 보냄
                    _flash_wait(key, code, f"📰 {hit}", f"   {title} ({src})\n   📎 원문: {link}\n   🔗 {FRONTEND_URL}/stock-analysis?code={code}", skey)
                    continue
                out.append({"key": key, "skey": skey, "text": f"📰 {hit}{_flash_quote_txt(code)}\n   {title} ({src})\n   📎 원문: {link}\n   🔗 {FRONTEND_URL}/stock-analysis?code={code}", "cat": "kr"})
        except Exception as e:
            print(f"[flash] 국내 뉴스 실패 {q}: {e}")
        time.sleep(0.2)
    return out


def _collect_flash_global() -> list:
    out = []
    comps = list(FLASH_GLOBAL_COMPANIES)
    queries = [
        ("en", "(" + " OR ".join(f'"{c}"' for c in comps) + ") (" + " OR ".join(FLASH_GLOBAL_EVENTS_EN) + ")", {"hl": "en-US", "gl": "US", "ceid": "US:en"}),
        ("ko", "(TSMC OR 마이크론 OR 엔비디아 OR ASML OR 애플 OR 테슬라 OR 인텔 OR 폭스콘) (" + " OR ".join(FLASH_GLOBAL_EVENTS_KO) + ")", {"hl": "ko", "gl": "KR", "ceid": "KR:ko"}),
    ]
    items = []
    for lang, q, loc in queries:
        try:
            res = requests.get("https://news.google.com/rss/search", params={"q": f"{q} when:1h", **loc}, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            for it in ET.fromstring(res.content).iter("item"):
                title = (it.findtext("title") or "").strip()
                src = (it.findtext("source") or "").strip()
                if src and title.endswith(f" - {src}"):
                    title = title[: -len(src) - 3]
                items.append((lang, title, src, (it.findtext("link") or "").strip()))
        except Exception as e:
            print(f"[flash] 해외 뉴스 실패: {e}")
    groups = {}  # (회사, 사건) → 같은 사건 기사 묶음
    today = now_kst().strftime("%Y-%m-%d")
    ko_alias = {"마이크론": "Micron", "엔비디아": "Nvidia", "애플": "Apple", "테슬라": "Tesla", "인텔": "Intel", "폭스콘": "Foxconn"}
    for lang, title, src, link in items:
        if FLASH_GLOBAL_FALSE.search(title):
            continue
        low = title.lower()
        comp = next((c for c in comps if re.search(rf"\b{re.escape(c.lower())}\b", low)), None) \
            or ko_alias.get(next((k for k in ko_alias if k in title), ""), None) or next((c for c in comps if c in title), None)
        event = next((v for k, v in FLASH_EVENT_WORDS.items() if k.isascii() and re.search(rf"\b{re.escape(k)}", low)), None) \
            or next((v for k, v in FLASH_EVENT_WORDS.items() if not k.isascii() and k in title), None)
        if not comp or not event:
            continue
        groups.setdefault((comp, event), []).append((lang, title, src, link))
    picked = []
    for (comp, event), arts in groups.items():
        key = f"glevent:{comp}:{event}:{today}"  # 같은 회사·같은 사건은 하루 한 번
        # 같은 사건이 며칠씩 기사로 이어져서 중복이 많았음 → 최근 3일 안에 보낸 같은 사건은 다시 안 보냄 (2026-10-26)
        recent = [f"glevent:{comp}:{event}:{(now_kst() - timedelta(days=d)).strftime('%Y-%m-%d')}" for d in (1, 2)]
        if _flash_seen(key) or any(_flash_seen(k) for k in recent) or any(p[0] == key for p in picked):
            continue
        picked.append((key, comp, event, arts))
    picked = picked[:8]
    # 번역은 실제로 보낼 기사 제목만 (예전엔 모든 기사를 번역해서 구글 번역이 막혔음)
    en_titles = [arts[0][1] for _, _, _, arts in picked if arts[0][0] == "en"]
    kos = iter(translate_to_ko(en_titles, src="en") if en_titles else [])
    for key, comp, event, arts in picked:
        lang, title, src, link = arts[0]
        ko = next(kos, None) if lang == "en" else None
        kr = FLASH_GLOBAL_COMPANIES.get(comp, "")
        more = f"\n   (같은 사건 기사 {len(arts) - 1}건 더)" if len(arts) > 1 else ""
        out.append({"key": key, "text": f"🌐 [{comp} · {event}] {title} ({src})" + (f"\n   ↳ {ko}" if ko else "") + more
                    + (f"\n   🇰🇷 관련: {kr}" if kr else "") + (f"\n   📎 원문: {link}" if link else ""), "cat": "gl"})
    return out[:8]


def _send_flash(items: list, title: str):
    kr = [i for i in items if i["cat"] == "kr"]
    gl = [i for i in items if i["cat"] == "gl"]
    for group, head in ((kr, "🚨 국내 증시 속보"), (gl, "🚨 해외 증시 속보")):
        if not group:
            continue
        for k in range(0, len(group), 8):  # 메시지가 너무 길지 않게 8개씩
            chunk = group[k:k + 8]
            text = f"{head}{title}\n━━━━━━━━━━━━\n" + "\n\n".join(i["text"] for i in chunk) + "\n\n※ 속보는 확인 전 1차 정보 — 원문·공시로 꼭 확인"
            if send_telegram(text, *bot("flash")):
                for i in chunk:
                    _flash_mark(i["key"])
                    if i.get("skey"):
                        _flash_mark(i["skey"])


def _run_flash_alerts():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    st = _flash_state
    if time.time() - st["at"] < 10 * 60:  # 10분마다 (5분은 너무 잦았음)
        return
    st["at"] = time.time()
    now = now_kst()
    # 주말(토·일)과 평일 새벽엔 보내지 않고 모아뒀다가 → 다음 평일 06:15 아침 알림에 같이 (2026-10-26)
    quiet = now.weekday() >= 5 or now.hour < 6 or (now.hour == 6 and now.minute < 30)
    items = []
    try:
        if now.weekday() < 5 and (7, 0) <= (now.hour, now.minute) <= (20, 30):
            items += _collect_flash_domestic()
        if now.weekday() < 5 and (9, 10) <= (now.hour, now.minute) <= (15, 20):
            items += _check_flash_waiting()  # 재료 뒤 늦게 거래대금이 터진 종목
        if now.weekday() < 5 and FLASH_GLOBAL_ENABLED:  # 해외 속보: 평일만 + 지금은 꺼둠 (FLASH_GLOBAL_ENABLED)
            items += _collect_flash_global()
    except Exception as e:
        print(f"[flash] 수집 실패: {e}")
    if quiet:
        known = {p["key"] for p in st["pending"]}
        st["pending"] = (st["pending"] + [i for i in items if i["key"] not in known])[-40:]
        return
    if st["pending"]:
        # 아침 알림(06:15)에 못 들어간 나머지는 한꺼번에 쏟아내지 않고 '본 것'으로만 표시 (중복·폭탄 알림 방지)
        for i in st["pending"]:
            _flash_mark(i["key"])
        st["pending"] = []
    if items:
        _send_flash(items, "")


@app.get("/api/flash-check")
def flash_check_endpoint(send: bool = False):
    """속보 미리보기 — send=true면 지금 바로 보냄 (보낸 건 다시 안 보냄)"""
    items = _collect_flash_domestic() + (_collect_flash_global() if FLASH_GLOBAL_ENABLED else [])
    if send and items:
        _send_flash(items, " (수동 확인)")
    return {"ok": True, "count": len(items), "items": [i["text"] for i in items]}


# ══════════════════════════════════════════════════════════════════════════
# 🧪 한투 시장별 투자자매매동향(일별) FHPTJ04040000 — KRX 단독 숫자와 비교해서
#    다른 곳(KRX+NXT 통합으로 보이는 숫자)과 같은지 확인하는 용도 (2026-10-23)
# 🔥 52주 신고가 — 한투 "국내주식 신고/신저근접종목 상위" FHPST01870000
# ══════════════════════════════════════════════════════════════════════════
@app.get("/api/kis-investor-daily")
def kis_investor_daily_endpoint(date: str = ""):
    """date=YYYYMMDD (비우면 오늘) — 코스피·코스닥 투자자별 순매수(한투) 원본. KRX 숫자와 비교용"""
    d = date or now_kst().strftime("%Y%m%d")
    out = {}
    for label, iscd, iscd1 in (("KOSPI", "0001", "KSP"), ("KOSDAQ", "1001", "KSQ")):
        try:
            data = kis_get("/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market", "FHPTJ04040000",
                           {"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": iscd, "FID_INPUT_DATE_1": d,
                            "FID_INPUT_ISCD_1": iscd1, "FID_INPUT_DATE_2": d, "FID_INPUT_ISCD_2": iscd})
            rows = data.get("output") or data.get("output1") or []
            if isinstance(rows, dict):
                rows = [rows]
            first = rows[0] if rows else {}
            # 금액 필드(…_ntby_tr_pbmn: 순매수 거래대금, 보통 백만원)만 골라 억원으로
            amt = {k: round(float(v) / 100, 1) for k, v in first.items() if k.endswith("ntby_tr_pbmn") and str(v).lstrip("-").replace(".", "").isdigit()}
            out[label] = {"rt_cd": data.get("rt_cd"), "msg": data.get("msg1"), "amount_eok": amt, "raw_first": first}
        except Exception as e:
            out[label] = {"error": str(e)}
    try:  # 같은 날 KRX(pykrx) 숫자도 같이
        for label in ("KOSPI", "KOSDAQ"):
            r = supabase.table("market_flow_trend").select("individual,foreign_amt,institution").eq("market", label) \
                .eq("trade_date", f"{d[:4]}-{d[4:6]}-{d[6:]}").limit(1).execute().data
            if r:
                out[label]["krx"] = r[0]
    except Exception:
        pass
    return {"ok": True, "date": d, "markets": out}


def fetch_new_highs(limit: int = 30, gap_max: str = "1") -> list:
    """52주 신고가 근접(괴리율 0~gap_max%) 종목 — 거래대금 순"""
    data = kis_get("/uapi/domestic-stock/v1/ranking/near-new-highlow", "FHPST01870000", {
        "fid_aply_rang_vol": "0", "fid_cond_mrkt_div_code": "J", "fid_cond_scr_div_code": "20187",
        "fid_div_cls_code": "0", "fid_input_cnt_1": "0", "fid_input_cnt_2": gap_max, "fid_prc_cls_code": "0",
        "fid_input_iscd": "0000", "fid_trgt_cls_code": "0", "fid_trgt_exls_cls_code": "0",
        "fid_aply_rang_prc_1": "", "fid_aply_rang_prc_2": ""})
    rows = data.get("output") or []
    out = []
    for r in rows:
        g = lambda *ks: next((r[k] for k in ks if r.get(k) not in (None, "")), None)
        code = g("mksc_shrn_iscd", "stck_shrn_iscd", "shrn_iscd")
        name = g("hts_kor_isnm", "hts_kor_isu_nm", "kor_isu_nm")  # 실제 응답 이름은 hts_kor_isnm (2026-10-23 확인)
        if not code or not name or THEME_SKIP_NAMES.search(name):
            continue
        # ETF·ETN은 빼기 (머니마켓 ETF 같은 게 '신고가'로 잡혔음 — 2026-10-26)
        if re.match(r"^(KODEX|TIGER|RISE|KBSTAR|ACE|SOL|HANARO|KOSEF|ARIRANG|PLUS|TIMEFOLIO|KIWOOM|KoAct|WON|BNK|UNICORN|마이티|파워|히어로즈|TREX|FOCUS|에셋플러스|VITA|DAISHIN343|1Q|TRUSTON)\b", name) \
                or re.search(r"ETN|머니마켓|액티브$", name):
            continue
        price = float(g("stck_prpr") or 0)
        vol = float(g("acml_vol") or 0)
        if vol <= 0:
            continue  # 거래 없는 종목(거래정지·장 마감 후 빈 값)은 제외
        val = float(g("acml_tr_pbmn") or 0) / 1e8 if g("acml_tr_pbmn") else price * vol / 1e8
        out.append({"code": code, "name": name, "price": price, "pct": float(g("prdy_ctrt") or 0), "value_eok": round(val, 1)})
    return sorted(out, key=lambda x: -x["value_eok"])[:limit]


def _save_new_highs_snapshot():
    """장 마감 직전(15:20~15:35) 52주 신고가를 저장 — 장 마감 뒤엔 이 순위가 비어서 16:10 수급 알림에 씀"""
    now = now_kst()
    if now.weekday() >= 5 or not ((15, 20) <= (now.hour, now.minute) <= (15, 35)):
        return
    key = now.strftime("%Y-%m-%d")
    if _brief_already_sent("job_new_highs", key):
        return
    try:
        items = fetch_new_highs(40)
        if items:
            supabase.table("brief_cache").upsert({"key": "new_highs_today", "data": {"date": key, "items": items}, "updated_at": now.isoformat()}).execute()
            _mark_brief_sent("job_new_highs", key)
    except Exception as e:
        print(f"[new_highs] 저장 실패: {e}")


def _new_highs_for_brief() -> list:
    try:
        r = supabase.table("brief_cache").select("data").eq("key", "new_highs_today").limit(1).execute().data
        if r and r[0]["data"].get("date") == now_kst().strftime("%Y-%m-%d"):
            return r[0]["data"]["items"]
    except Exception:
        pass
    return fetch_new_highs(40)


@app.get("/api/new-highs")
def new_highs_endpoint(raw: bool = False):
    try:
        if raw:
            data = kis_get("/uapi/domestic-stock/v1/ranking/near-new-highlow", "FHPST01870000", {
                "fid_aply_rang_vol": "0", "fid_cond_mrkt_div_code": "J", "fid_cond_scr_div_code": "20187",
                "fid_div_cls_code": "0", "fid_input_cnt_1": "0", "fid_input_cnt_2": "1", "fid_prc_cls_code": "0",
                "fid_input_iscd": "0000", "fid_trgt_cls_code": "0", "fid_trgt_exls_cls_code": "0",
                "fid_aply_rang_prc_1": "", "fid_aply_rang_prc_2": ""})
            return {"ok": True, "rt_cd": data.get("rt_cd"), "msg": data.get("msg1"), "first3": (data.get("output") or [])[:3]}
        return {"ok": True, "items": fetch_new_highs()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


_minute_mem: dict = {}


def _minute_day_cached(code: str, day: str, market: str = "J") -> dict:
    """⚡ 지난 날짜 1분봉은 한 번 받으면 보관 (메모리 → DB → 한투 순서). 오늘은 장중에 바뀌니 짧게만 (2026-10-28)
    — 분봉 화면이 느리던 가장 큰 이유가 매번 날짜마다 한투에 여러 번씩 물어보던 것"""
    key = (code, day) if market == "J" else (code, day, market)
    today = now_kst().strftime("%Y-%m-%d")
    hit = _minute_mem.get(key)
    if hit and (day < today or time.time() - hit[0] < 60):
        return hit[1]
    if market != "J":  # 🌙 통합은 DB 보관 없이 메모리에만
        out = stock_minute_by_date_endpoint(code, day, market)
        if out.get("ok") and out.get("candles"):
            _minute_mem[key] = (time.time(), out)
        return out
    if day < today:
        try:
            r = supabase.table("minute_candles_cache").select("candles").eq("code", code).eq("day", day).limit(1).execute().data
            if r and r[0]["candles"]:
                out = {"ok": True, "candles": r[0]["candles"], "cached": True}
                _minute_mem[key] = (time.time(), out)
                return out
        except Exception:
            pass
    out = stock_minute_by_date_endpoint(code, day)
    if out.get("ok") and out.get("candles"):
        _minute_mem[key] = (time.time(), out)
        if day < today and len(out["candles"]) >= 300:  # 하루치가 다 찼을 때만 보관
            try:
                supabase.table("minute_candles_cache").upsert({"code": code, "day": day, "candles": out["candles"]}).execute()
            except Exception:
                pass
    if len(_minute_mem) > 3000:
        _minute_mem.clear()
    return out


@app.get("/api/stock-minute-multi")
def stock_minute_multi_endpoint(code: str, days: int = 5, end: str = "", start: str = "", mode: str = "krx"):
    """여러 날 1분봉 — start~end(YYYY-MM-DD, end 비우면 오늘) 또는 end까지 최근 N거래일. 최대 10거래일 (2026-10-26)"""
    e = datetime.strptime(end, "%Y-%m-%d") if end else now_kst().replace(tzinfo=None)
    if start:
        st = datetime.strptime(start, "%Y-%m-%d")
        dates, d = [], st
        while d <= e and len(dates) < 10:
            if d.weekday() < 5:
                dates.append(d)
            d += timedelta(days=1)
    else:
        need, dates, d, tries = max(1, min(int(days), 10)), [], e, 0
        while len(dates) < need and tries < need + 8:
            tries += 1
            if d.weekday() < 5:
                dates.append(d)
            d -= timedelta(days=1)
        dates.reverse()
    import concurrent.futures as cf
    days = [d.strftime("%Y-%m-%d") for d in dates]
    with cf.ThreadPoolExecutor(max_workers=4) as ex:  # 날짜별로 동시에 (한투 초당 제한 안쪽)
        mk = "UN" if mode == "al" else "J"  # 🔀 2026-11-07
        results = list(ex.map(lambda dd: _minute_day_cached(code, dd, mk), days))
    out = []
    for dd, r in zip(days, results):
        if r.get("ok") and r.get("candles"):
            out.append({"date": dd, "candles": r["candles"]})
    return {"ok": bool(out), "code": code, "days": out, "mode": "al" if mode == "al" else "krx", "source": "한투",
            "error": None if out else "분봉 데이터가 없어요 (휴장일이거나 너무 오래된 날짜)"}


# ══════════════════════════════════════════════════════════════════════════
# 🎯 분봉 세력평단 근사치 보정 — 정밀값(실시간 체결)이 있는 날, 같은 구간의 근사치와 비교해
#    "정밀 ÷ 근사" 비율을 종목별로 모아두고, 여러 날(근사치) 조회 때 곱해서 보정값을 보여줌 (2026-10-26)
# ══════════════════════════════════════════════════════════════════════════
@app.get("/api/minute-calib")
def minute_calib_get(code: str):
    try:
        rows = supabase.table("minute_calibration").select("*").execute().data or []
        me = next((r for r in rows if r["code"] == code), None)
        allr = [r for r in rows if r["n"] > 0]
        gn = sum(r["n"] for r in allr)
        glob = (sum(r["ratio_sum"] for r in allr) / gn) if gn else None
        out = {"ok": True, "code": code, "global_ratio": glob, "global_n": gn}
        if me and me["n"] > 0:
            mean = me["ratio_sum"] / me["n"]
            var = max(0.0, me["ratio_sq"] / me["n"] - mean * mean)
            out.update({"ratio": mean, "n": me["n"], "sd": var ** 0.5, "min": me["ratio_min"], "max": me["ratio_max"]})
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/minute-calib")
def minute_calib_post(payload: dict):
    """{code, precise, approx} — 같은 구간의 정밀값과 근사치를 받아 비율을 쌓음 (하루 같은 구간은 1번만)"""
    try:
        code, p, a = payload.get("code"), float(payload.get("precise") or 0), float(payload.get("approx") or 0)
        if not code or p <= 0 or a <= 0:
            return {"ok": False, "error": "값이 없어요"}
        r = p / a
        if not (0.5 < r < 1.5):
            return {"ok": False, "error": "비율이 너무 커서 무시 (데이터 이상)"}
        key = f"{code}:{payload.get('day', '')}:{payload.get('start', '')}"
        if _brief_already_sent("minute_calib", key):
            return {"ok": True, "skipped": True}
        cur = supabase.table("minute_calibration").select("*").eq("code", code).limit(1).execute().data
        c = cur[0] if cur else {"code": code, "n": 0, "ratio_sum": 0, "ratio_sq": 0, "ratio_min": None, "ratio_max": None}
        c.update({"n": c["n"] + 1, "ratio_sum": c["ratio_sum"] + r, "ratio_sq": c["ratio_sq"] + r * r,
                  "ratio_min": r if c["ratio_min"] is None else min(c["ratio_min"], r),
                  "ratio_max": r if c["ratio_max"] is None else max(c["ratio_max"], r), "updated_at": now_kst().isoformat()})
        supabase.table("minute_calibration").upsert(c).execute()
        _mark_brief_sent("minute_calib", key)
        return {"ok": True, "ratio": r, "n": c["n"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 🗺 오늘의 시장 지도 (테마 기준) — 크기 = 오늘 거래대금, 색 = 등락률, 묶음 = 오늘 돈이 몰린 테마 (2026-10-26)
#   업종(네이버 업종)이 너무 거칠고, 시가총액 크기는 삼성·하이닉스만 커 보여서 교체
# ══════════════════════════════════════════════════════════════════════════
_theme_map_cache = {"at": 0.0, "data": None}


def _theme_map_build(top: int = 14) -> dict:
    """테마 30개를 동시에(8개씩) 훑어서 지도 데이터 만들기 — 예전엔 하나씩 40개를 훑어 너무 오래 걸려 '서버 연결 실패'가 났음 (2026-10-28)"""
    import concurrent.futures as cf
    themes = [t for t in fetch_naver_theme_list(limit=30) if not THEME_SKIP_NAMES.search(t["theme_name"])]

    def one(t):
        try:
            stocks, _ = fetch_naver_theme_detail(t["theme_no"], limit=30)
        except Exception:
            return None
        items = []
        for x in stocks:
            eok = _to_eok_from_naver(x["trading_value"], x.get("price", 0), x.get("volume", 0))
            if eok >= 10:
                items.append({"name": x["stock_name"], "code": x["stock_code"], "pct": round(x["change_pct"], 2), "eok": round(eok, 1)})
        if not items:
            return None
        up_money = sum(i["eok"] for i in items if i["pct"] > 0)
        return {"theme": t["theme_name"], "pct": round(t["change_pct"], 2), "money": round(sum(i["eok"] for i in items), 1),
                "score": up_money * (1 + max(0, t["change_pct"]) / 10), "stocks": sorted(items, key=lambda i: -i["eok"])[:10]}

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        out = [r for r in ex.map(one, themes) if r]
    out.sort(key=lambda x: -x["score"])
    picked, used = [], set()
    for th in out:  # 같은 종목은 한 테마에만 (돈이 가장 많이 몰린 테마 쪽으로)
        th["stocks"] = [i for i in th["stocks"] if i["code"] not in used]
        if th["stocks"]:
            used.update(i["code"] for i in th["stocks"])
            picked.append(th)
        if len(picked) >= top:
            break
    return {"ok": True, "at": now_kst().strftime("%H:%M"), "themes": picked, "note": "KRX 기준(넥스트레이드 제외) · 3분마다 갱신"}


_theme_map_lock = threading.Lock()


def _theme_map_refresh():
    if not _theme_map_lock.acquire(blocking=False):
        return  # 이미 만드는 중
    try:
        data = _theme_map_build()
        if data["themes"]:
            _theme_map_cache.update({"at": time.time(), "data": data})
    except Exception as e:
        print(f"[theme_map] 갱신 실패: {e}")
    finally:
        _theme_map_lock.release()


def _run_theme_map_job():
    """장중(08:58~15:40) 3분마다 서버가 미리 만들어 둠 → 화면은 바로 뜸"""
    now = now_kst()
    if now.weekday() >= 5 or not ((8, 58) <= (now.hour, now.minute) <= (15, 40)):
        return
    if time.time() - _theme_map_cache["at"] >= 170:
        _theme_map_refresh()


@app.get("/api/theme-map")
def theme_map_endpoint(top: int = 14, refresh: bool = False):
    c = _theme_map_cache
    if c["data"] and not refresh:
        if time.time() - c["at"] >= 180:
            threading.Thread(target=_theme_map_refresh, daemon=True).start()  # 오래된 건 일단 보여주고 뒤에서 새로
        return c["data"]
    try:
        _theme_map_refresh()
        return c["data"] or {"ok": False, "error": "테마 데이터를 아직 못 만들었어요 (잠시 후 다시)"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 📈 코스피200 선물 미결제약정 · 베이시스 — 한투 '선물옵션 시세' FHMIF10000000 (최근월물 101000) (2026-10-26)
#   응답 칸 이름이 문서마다 조금 달라서 여러 후보로 읽고, 베이시스가 없으면 '선물 − 코스피200'으로 계산
# ══════════════════════════════════════════════════════════════════════════
_fut_basis_cache = {"at": 0.0, "data": None}


def _save_futures_close():
    """📸 장 마감 직후(15:40~15:55) 선물 미결제약정·베이시스를 저장 → 장 마감 뒤엔 이 값을 보여줌 (2026-10-27)
    (마감 뒤 실시간 값은 야간 선물 vs 마감 지수라 리포트에 쓰면 헷갈림)"""
    now = now_kst()
    if now.weekday() >= 5 or not ((15, 40) <= (now.hour, now.minute) <= (15, 55)):
        return
    key = now.strftime("%Y-%m-%d")
    if _brief_already_sent("job_fut_close", key):
        return
    _fut_basis_cache["at"] = 0  # 새로 받기
    d = futures_basis_endpoint(live_only=True)
    if d.get("ok") and d.get("basis") is not None:
        supabase.table("brief_cache").upsert({"key": "fut_close", "data": {**d, "date": key, "saved_at": now.strftime("%H:%M")},
                                              "updated_at": now.isoformat()}).execute()
        _mark_brief_sent("job_fut_close", key)


@app.get("/api/futures-basis")
def futures_basis_endpoint(iscd: str = "101000", raw: bool = False, live_only: bool = False):
    now = now_kst()
    regular = now.weekday() < 5 and (9, 0) <= (now.hour, now.minute) <= (15, 45)
    if not raw and not live_only and not regular:
        # 장 마감 뒤·장 전: 마감 직후 저장해 둔 값 + 지금 야간 선물은 참고로
        try:
            r = supabase.table("brief_cache").select("data").eq("key", "fut_close").limit(1).execute().data
            if r:
                snap = dict(r[0]["data"])
                night = futures_basis_endpoint(iscd, live_only=True)
                snap.update({"closed": True, "night_price": night.get("price"), "night_basis": night.get("basis")})
                return snap
        except Exception:
            pass
    c = _fut_basis_cache
    if not raw and c["data"] and time.time() - c["at"] < 60:
        return c["data"]
    try:
        data = kis_get("/uapi/domestic-futureoption/v1/quotations/inquire-price", "FHMIF10000000",
                       {"FID_COND_MRKT_DIV_CODE": "F", "FID_INPUT_ISCD": iscd})
        if raw:
            return {"ok": True, "raw": data}
        if data.get("rt_cd") != "0":
            return {"ok": False, "error": data.get("msg1")}
        o1 = data.get("output1") or {}
        o3 = data.get("output3") or {}
        f = lambda d, *ks: next((float(str(d[k]).replace(",", "")) for k in ks if d.get(k) not in (None, "")), None)
        price = f(o1, "futs_prpr")
        chg = f(o1, "futs_prdy_vrss") or 0
        if o1.get("prdy_vrss_sign") in ("4", "5"):
            chg = -abs(chg)
        oi = f(o1, "hts_otst_stpl_qty", "otst_stpl_qty")
        oi_chg = f(o1, "otst_stpl_qty_icdc", "hts_otst_stpl_qty_icdc")
        k200 = f(o3, "bstp_nmix_prpr", "kospi200_nmix") or f(o1, "kospi200_nmix")
        # 실제 응답 확인(2026-10-26): mrkt_basis = 시장 베이시스, basis = 이론 베이시스, dprt = 괴리율(%)
        basis = f(o1, "mrkt_basis")
        if basis is None and price and k200:
            basis = round(price - k200, 2)
        theo = f(o1, "hts_thpr", "thpr")
        theo_basis = f(o1, "basis")
        if theo_basis is None and theo and k200:
            theo_basis = round(theo - k200, 2)
        out = {"ok": True, "at": now_kst().strftime("%H:%M"), "name": o1.get("hts_kor_isnm", "코스피200 선물"),
               "price": price, "change": chg, "kospi200": k200, "basis": basis, "theory": theo,
               "theory_basis": theo_basis, "gap_pct": f(o1, "dprt"), "days_left": f(o1, "hts_rmnn_dynu"),
               "oi": oi, "oi_change": oi_chg}
        c.update({"at": time.time(), "data": out})
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 🧺 ETF 투자자별 순매수 TOP — "금요일 ETF로 돈이 들어오면 월요일 그 섹터로 돈이 들어온다" (JM 관찰, 2026-10-26)
#   · ETF 목록: 네이버 ETF 목록(국내 업종·테마, 레버리지·인버스 제외) 거래대금 상위 60개
#   · 투자자별 순매수: 한투 '주식현재가 투자자'(FHKST01010900) — 가장 최근 확정일 (장중엔 전 거래일)
#   · ETF 누르면 구성종목: 한투 'ETF 구성종목시세'(FHKST121600C0) + 종목별 외국인·기관(확정→없으면 장중 추정)·프로그램
# ══════════════════════════════════════════════════════════════════════════
_etf_cache: dict = {}
ETF_SKIP = re.compile(r"레버리지|인버스|2X|선물|커버드콜|채권|금리|머니마켓|CD|KOFR|합성")


def _etf_universe(n: int = 60) -> list:
    c = _etf_cache.get("universe")
    if c and time.time() - c[0] < 3600:
        return c[1]
    res = requests.get("https://finance.naver.com/api/sise/etfItemList.nhn", params={"etfType": "0"},
                       headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/sise/etf.naver"}, timeout=12)
    try:
        j = res.json()
    except Exception:
        j = json.loads(res.content.decode("euc-kr", errors="replace"))
    items = (j.get("result") or {}).get("etfItemList") or []
    out = []
    for it in items:
        if str(it.get("etfTabCode")) != "2" or ETF_SKIP.search(it.get("itemname", "")):
            continue
        out.append({"code": it["itemcode"], "name": it["itemname"], "pct": float(it.get("changeRate") or 0),
                    "price": float(it.get("nowVal") or 0), "value_eok": round(float(it.get("amonut") or 0) / 100, 1)})
    out.sort(key=lambda x: -x["value_eok"])
    out = out[:n]
    _etf_cache["universe"] = (time.time(), out)
    return out


def _etf_all() -> list:
    c = _etf_cache.get("all")
    if c and time.time() - c[0] < 3600:
        return c[1]
    res = requests.get("https://finance.naver.com/api/sise/etfItemList.nhn", params={"etfType": "0"},
                       headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/sise/etf.naver"}, timeout=12)
    try:
        j = res.json()
    except Exception:
        j = json.loads(res.content.decode("euc-kr", errors="replace"))
    items = (j.get("result") or {}).get("etfItemList") or []
    _etf_cache["all"] = (time.time(), items)
    return items


# 📊 섹터 대표 ETF — 섹터마다 하나씩, 시장 색깔을 한눈에 (이름으로 찾아서 코드가 바뀌어도 안전)
SECTOR_ETFS = [
    ("코스피", "KODEX 200"), ("코스닥", "KODEX 코스닥150"), ("반도체", "TIGER 반도체TOP10"), ("반도체 소부장", "SOL AI반도체소부장"),
    ("바이오", "KoAct 바이오헬스케어액티브"), ("2차전지", "KODEX 2차전지산업"), ("자동차", "KODEX 자동차"), ("조선", "SOL 조선TOP3플러스"),
    ("방산", "PLUS K방산"), ("AI 전력", "KODEX AI전력핵심설비"), ("원자력", "TIGER 코리아원자력"), ("로봇", "KODEX 로봇액티브"),
    ("우주항공", "PLUS 우주항공"), ("태양광·ESS", "PLUS 태양광&ESS"), ("신재생", "KODEX 신재생에너지액티브"), ("수소", "RISE 수소경제테마"),
    ("화장품", "TIGER 화장품"), ("K-POP", "ACE KPOP포커스"), ("건설", "KODEX 건설"), ("은행", "TIGER 은행고배당플러스TOP10"),
    ("증권", "KODEX 증권"), ("인터넷", "TIGER 인터넷TOP10"), ("게임", "KODEX 게임산업"), ("철강", "KODEX 철강"),
]


@app.get("/api/etf-sectors")
def etf_sectors_endpoint():
    c = _etf_cache.get("sectors")
    if c and time.time() - c[0] < 120:
        return c[1]
    try:
        items = _etf_all()
        by_name = {it["itemname"].replace(" ", ""): it for it in items}
        rows = []
        for sector, nm in SECTOR_ETFS:
            key = nm.replace(" ", "")
            it = by_name.get(key) or next((v for k, v in by_name.items() if k.startswith(key)), None)
            if not it:
                continue
            code = it["itemcode"]
            row = {"sector": sector, "code": code, "name": it["itemname"], "pct": float(it.get("changeRate") or 0),
                   "price": float(it.get("nowVal") or 0), "value_eok": round(float(it.get("amonut") or 0) / 100, 1),
                   "cap_eok": float(it.get("marketSum") or 0)}
            try:
                q = _kis_quote(code)
                row.update({"price": q["price"] or row["price"], "pct": q["pct"], "open": q["open"], "high": q["high"], "low": q["low"]})
            except Exception:
                pass
            try:
                ck = f"inv:{code}"
                ic = _etf_cache.get(ck)
                if not ic or time.time() - ic[0] > 600:
                    ic = (time.time(), _investor_daily(code))
                    _etf_cache[ck] = ic
                if ic[1]:
                    row.update({"foreign": ic[1]["foreign"], "institution": ic[1]["institution"], "inv_date": ic[1]["date"]})
            except Exception:
                pass
            rows.append(row)
            time.sleep(0.05)
        body = [r for r in rows if r["sector"] not in ("코스피", "코스닥")]
        up = sum(1 for r in body if r["pct"] > 0)
        strong = [r["sector"] for r in sorted(body, key=lambda r: -r["pct"])[:3]]
        weak = [r["sector"] for r in sorted(body, key=lambda r: r["pct"])[:3]]
        mood = ("🔴 대부분 상승 — 시장 전체가 강함" if up >= len(body) * 0.75 else
                "🔵 대부분 하락 — 시장 색깔 없음, 관망" if up <= len(body) * 0.25 else
                "🟡 섹터별로 갈림 — 강한 섹터 위주로")
        out = {"ok": True, "at": now_kst().strftime("%H:%M"), "rows": rows,
               "summary": {"up": up, "down": len(body) - up, "strong": strong, "weak": weak, "mood": mood}}
        _etf_cache["sectors"] = (time.time(), out)
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _investor_daily(code: str):
    """가장 최근 확정일의 개인·외국인·기관 순매수(억) — 장중엔 오늘 줄이 비어서 전 거래일"""
    data = kis_get("/uapi/domestic-stock/v1/quotations/inquire-investor", "FHKST01010900",
                   {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code})
    for r in data.get("output") or []:
        f = lambda k: float(r.get(k) or 0)
        p, fr, o = f("prsn_ntby_tr_pbmn"), f("frgn_ntby_tr_pbmn"), f("orgn_ntby_tr_pbmn")
        if p or fr or o:
            d = r.get("stck_bsop_date", "")
            return {"date": f"{d[:4]}-{d[4:6]}-{d[6:]}", "individual": round(p / 100, 1), "foreign": round(fr / 100, 1), "institution": round(o / 100, 1)}
    return None


@app.get("/api/etf-flow-top")
def etf_flow_top_endpoint(who: str = "foreign", side: str = "buy", n: int = 30):
    key = "flows"
    c = _etf_cache.get(key)
    try:
        if not c or time.time() - c[0] > 600:
            rows = []
            for e in _etf_universe():
                try:
                    inv = _investor_daily(e["code"])
                except Exception:
                    inv = None
                if inv:
                    rows.append({**e, **inv})
                time.sleep(0.06)
            c = (time.time(), rows)
            _etf_cache[key] = c
        rows = c[1]
        k = {"foreign": "foreign", "institution": "institution", "individual": "individual"}.get(who, "foreign")
        rows = sorted(rows, key=lambda r: -r[k] if side == "buy" else r[k])[: max(5, min(n, 60))]
        date = max((r["date"] for r in rows), default="")
        return {"ok": True, "who": k, "side": side, "date": date, "rows": rows,
                "note": "국내 업종·테마 ETF(레버리지·인버스 제외) 거래대금 상위 60개 중 · 가장 최근 확정일 기준"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/etf-components")
def etf_components_endpoint(code: str, n: int = 15):
    ck = f"comp:{code}"
    c = _etf_cache.get(ck)
    if c and time.time() - c[0] < 180:
        return c[1]
    try:
        data = kis_get("/uapi/etfetn/v1/quotations/inquire-component-stock-price", "FHKST121600C0",
                       {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_COND_SCR_DIV_CODE": "11216"})
        comp = data.get("output2") or []
        g = lambda r, *ks: next((r[k] for k in ks if r.get(k) not in (None, "")), None)
        items = []
        for r in comp:
            sc = g(r, "stck_shrn_iscd", "mksc_shrn_iscd")
            if not sc or not str(sc).isdigit():
                continue  # 현금·선물 등 제외
            items.append({"code": sc, "name": g(r, "hts_kor_isnm"), "pct": float(g(r, "prdy_ctrt") or 0),
                          "weight": float(g(r, "etf_cnfg_issu_rlim", "etf_vltn_amt") or 0)})
        items.sort(key=lambda x: -x["weight"])
        items = items[: max(5, min(n, 25))]
        now = now_kst()
        live = now.weekday() < 5 and (9, 0) <= (now.hour, now.minute) <= (15, 40)
        for it in items:
            it["foreign"] = it["institution"] = it["program"] = None
            it["basis"] = ""
            try:
                if live:
                    est = stock_live_flow_endpoint(it["code"])
                    if est.get("ok") and est.get("slots"):
                        last = est["slots"][-1]
                        it.update({"foreign": last.get("foreign_eok"), "institution": last.get("institution_eok"), "basis": "장중 추정"})
                if it["foreign"] is None:
                    inv = _investor_daily(it["code"])
                    if inv:
                        it.update({"foreign": inv["foreign"], "institution": inv["institution"], "basis": inv["date"][5:]})
            except Exception:
                pass
            try:
                pr = fetch_program_trade_daily(it["code"])
                it["program"] = pr[0]["net_amt"] if pr else None
            except Exception:
                pass
            time.sleep(0.05)
        out = {"ok": True, "code": code, "rows": items, "at": now.strftime("%H:%M"), "rt_cd": data.get("rt_cd"), "msg": data.get("msg1")}
        _etf_cache[ck] = (time.time(), out)
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/test-bot")
def test_bot_endpoint(channel: str = "flash"):
    """봇 연결 테스트 — channel=main/report/news/flash/sports 로 '테스트' 메시지 한 통 보내기 (2026-10-26)"""
    names = {"main": "JM 전용", "report": "JM 리포트", "news": "뉴스", "flash": "공시·속보", "sports": "스포츠"}
    tok, chat = bot(channel)
    if channel != "main" and not tok:
        return {"ok": False, "channel": channel, "error": f"Render에 TELEGRAM_BOT_TOKEN_{channel.upper()} 가 없어요"}
    text = f"🧪🦀【테스트】 {names.get(channel, channel)} 봇 연결 확인\n━━━━━━━━━━━━\n이 메시지가 보이면 정상이에요 ✅ ({now_kst().strftime('%m/%d %H:%M')})"
    sent = send_telegram(text, tok, chat)
    return {"ok": True, "channel": channel, "sent": sent, "send_error": None if sent else _tg_last_error["msg"]}


# ══════════════════════════════════════════════════════════════════════════
# 📊 매크로 지수 시세 — 등락률을 "전 거래일 종가 대비"로 정확하게 (2026-10-27)
#   예전 화면 시세는 며칠 전 값과 비교돼서 코스피 −1.46%가 −5.18%로 나왔음 (리포트에 그대로 옮겨질 뻔)
# ══════════════════════════════════════════════════════════════════════════
_mq_cache: dict = {}


def _macro_quote(sym: str) -> dict:
    c = _mq_cache.get(sym)
    if c and time.time() - c[0] < 25:
        return c[1]
    j = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}",
                     params={"range": "5d", "interval": "1d"}, headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
    res = j["chart"]["result"][0]
    meta = res.get("meta") or {}
    closes = [x for x in (res["indicators"]["quote"][0].get("close") or []) if x is not None]
    price = meta.get("regularMarketPrice") or (closes[-1] if closes else None)
    prev = closes[-2] if len(closes) >= 2 else meta.get("previousClose") or meta.get("chartPreviousClose")
    if not price or not prev:
        raise ValueError("시세 없음")
    out = {"symbol": sym, "ok": True, "price": price, "change": price - prev, "changePct": (price - prev) / prev * 100, "prev": prev}
    _mq_cache[sym] = (time.time(), out)
    return out


# ⚡ 매크로 빠르게 (2026-11-02): 화면이 쓰는 종목 목록을 기억해 두고, 서버가 장중 1분(밤·주말 5분)마다
#    한꺼번에(8개씩 동시에) 미리 받아둠 → 화면은 보관된 값을 바로 가져감. 숫자 계산은 예전과 똑같음
_mq_symbols: set = set()
_mq_fresh = {"at": 0.0}


def _mq_refresh(syms):
    import concurrent.futures as cf

    def one(sym):
        try:
            _macro_quote(sym)  # 25초 지난 건 알아서 새로 받음 (실패해도 예전 값은 남겨둠)
        except Exception:
            pass

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(one, list(syms)))
    _mq_fresh["at"] = time.time()


def _mq_loop():
    time.sleep(20)
    while True:
        try:
            now = now_kst()
            busy = now.weekday() < 5 and ((8, 30) <= (now.hour, now.minute) <= (15, 40) or now.hour >= 21 or now.hour < 6)
            if _mq_symbols:
                _mq_refresh(set(_mq_symbols))
            time.sleep(60 if busy else 300)
        except Exception as e:
            print(f"[macro] 미리 받기 실패: {e}")
            time.sleep(60)


# ══════════════════════════════════════════════════════════════════════════
# 🏛 매크로 지표 1순위 (2026-11-10 JM) — 연준·물가·고용·성장·위험 신호·한국 + 에너지·금속 추가
#   발표형 지표는 FRED(연준 공식) · 한국은 한국은행 ECOS(ECOS_API_KEY) · 공급망은 뉴욕연은
#   각 지표마다 "무슨 뜻인지(meaning)" · "보는 법(read)"을 같이 내려줌 → 화면 설명 칸
# ══════════════════════════════════════════════════════════════════════════
ECOS_API_KEY = os.environ.get("ECOS_API_KEY")

# (묶음, 키, 이름, 출처, 시리즈, 변환, 단위, 주기, 뜻, 보는 법)
#   변환: level 그대로 · yoy 전년비% · mom 전월비% · diff 전월 대비 증감 · scale:N 나누기
MACRO_IND = [
    ("fed", "fed_rate", "연준 기준금리", "fed_range", None, "level", "%", "FOMC 연 8회",
     "연준이 정하는 미국 기준금리 (목표 범위)", "인상 = 긴축 · 동결 · 인하 = 완화 — 세계 돈의 가격"),
    ("fed", "effr", "실효 연방기금금리", "fred", "EFFR", "level", "%", "매일",
     "은행끼리 하루 동안 실제로 빌린 금리", "기준금리 범위 안이면 정상 · 위쪽에 붙으면 돈이 빠듯"),
    ("fed", "iorb", "지급준비금 이자 (IORB)", "fred", "IORB", "level", "%", "FOMC 때",
     "은행이 연준에 맡긴 돈에 주는 이자", "사실상 단기 금리의 바닥을 받쳐주는 장치"),
    ("fed", "dgs2", "美 2년물", "fred", "DGS2", "level", "%", "매일",
     "연준이 앞으로 금리를 어떻게 할지 시장 예상이 가장 잘 담긴 금리", "오르면 '더 올리거나 오래 높게' 예상 · 내리면 인하 기대"),
    ("fed", "walcl", "연준 총자산 (대차대조표)", "fred", "WALCL", "scale:1000000", "조달러", "매주 목",
     "연준이 가진 국채·MBS 등 전체 자산", "줄어들면 QT = 시중 돈 회수(긴축) · 늘면 돈 풀기"),
    ("fed", "rrp", "역레포 잔액", "fred", "RRPONTSYD", "scale:1", "십억달러", "매일",
     "갈 곳 없는 돈이 연준에 하룻밤 맡겨진 금액", "줄어들수록 시중 여유 자금이 줄어드는 중"),
    ("fed", "reserves", "은행 지급준비금", "fred", "WRESBAL", "scale:1000", "조달러", "매주",
     "은행들이 연준에 쌓아둔 돈", "너무 줄면 단기 자금시장이 경색 → 연준이 QT를 멈추는 신호"),

    ("infl", "core_pce", "근원 PCE (전년비)", "fred", "PCEPILFE", "yoy", "%", "매월 말",
     "식품·에너지 뺀 개인소비지출 물가 — 연준 2% 목표의 공식 기준", "2%보다 높을수록 매파 · 2%에 가까워지면 인하 근거"),
    ("infl", "pce", "PCE (전년비)", "fred", "PCEPI", "yoy", "%", "매월 말",
     "개인이 실제로 쓰는 돈 기준 전체 물가", "에너지·식품 포함이라 근원보다 출렁임이 큼"),
    ("infl", "cpi", "CPI (전년비)", "fred", "CPIAUCSL", "yoy", "%", "매월 중순",
     "소비자물가 — 시장이 가장 크게 반응하는 물가", "예상보다 높으면 주식 ↓·달러 ↑ · 낮으면 반대"),
    ("infl", "core_cpi", "근원 CPI (전년비)", "fred", "CPILFESL", "yoy", "%", "매월 중순",
     "식품·에너지 뺀 소비자물가", "일시적 출렁임을 뺀 '진짜 물가 흐름'"),
    ("infl", "ppi", "PPI 최종수요 (전년비)", "fred", "PPIFIS", "yoy", "%", "매월 중순",
     "생산자물가 — 공장·도매 단계 가격", "CPI·PCE보다 먼저 움직이는 선행 지표"),
    ("infl", "bei5", "5년 기대인플레 (BEI)", "fred", "T5YIE", "level", "%", "매일",
     "국채와 물가연동채 금리 차이로 계산한 시장의 5년 물가 예상", "오르면 시장이 물가를 걱정 · 기대 경로 체크"),
    ("infl", "bei5y5", "장기 기대인플레 (5년 뒤 5년)", "fred", "T5YIFR", "level", "%", "매일",
     "5년 뒤부터 5년간의 물가 예상 — 연준이 가장 지키려는 숫자", "2~2.5%에서 벗어나 오르면 연준이 긴장"),
    ("infl", "mich", "미시간대 1년 기대인플레", "fred", "MICH", "level", "%", "매월",
     "소비자들이 예상하는 1년 뒤 물가", "오르면 '물가가 계속 오를 거야' 심리 → 임금·가격 선반영 위험"),

    ("job", "nfp", "비농업 고용 (증가)", "fred", "PAYEMS", "diff", "천명", "매월 첫 금요일",
     "한 달 동안 늘어난 일자리 수", "예상보다 많으면 경기 강함 = 매파 · 적거나 마이너스면 비둘기파"),
    ("job", "unrate", "실업률", "fred", "UNRATE", "level", "%", "매월 첫 금요일",
     "일하고 싶은데 일자리가 없는 사람 비율", "오르기 시작하면 경기 둔화 신호 → 인하 압력"),
    ("job", "ahe", "시간당 임금 (전년비)", "fred", "CES0500000003", "yoy", "%", "매월 첫 금요일",
     "평균 시간당 임금 상승률", "높으면 임금발 물가(끈적한 서비스 물가) 우려"),
    ("job", "jolts", "JOLTS 구인 건수", "fred", "JTSJOL", "scale:1", "천건", "매월",
     "기업이 뽑으려고 내놓은 일자리 수", "실업자 수보다 많으면 일손 부족 → 임금 ↑"),
    ("job", "claims", "신규 실업수당 청구", "fred", "ICSA", "scale:1000", "천건", "매주 목",
     "한 주 동안 새로 실업수당을 신청한 사람", "가장 빠른 고용 신호 · 꾸준히 늘면 경고"),

    ("growth", "gdp", "실질 GDP 성장률 (연율)", "fred", "A191RL1Q225SBEA", "level", "%", "분기",
     "미국 경제 전체가 얼마나 성장했나 (분기, 연율 환산)", "2% 안팎이 보통 · 마이너스 2분기 연속이면 침체 얘기"),
    ("growth", "retail", "소매판매 (전월비)", "fred", "RSAFS", "mom", "%", "매월 중순",
     "가게·온라인에서 팔린 금액", "소비 = 미국 경제의 약 70% · 강하면 경기 탄탄"),
    ("growth", "indpro", "산업생산 (전월비)", "fred", "INDPRO", "mom", "%", "매월 중순",
     "공장·광산·전기 생산량", "제조업 경기 · 플러스면 생산 확장"),
    ("growth", "houst", "주택착공", "fred", "HOUST", "scale:1", "천호(연율)", "매월",
     "새로 짓기 시작한 집", "금리에 가장 민감 — 금리 ↑ 땐 먼저 줄어듦"),

    ("risk", "t10y2y", "장단기 금리차 (10년−2년)", "fred", "T10Y2Y", "level", "%p", "매일",
     "장기 금리 − 단기 금리", "마이너스(역전) = 침체 경고 · 역전이 풀리며 침체가 오는 경우가 많음"),
    ("risk", "t10y3m", "장단기 금리차 (10년−3개월)", "fred", "T10Y3M", "level", "%p", "매일",
     "연준이 침체 예측에 더 신뢰하는 금리차", "역전이 길어질수록 침체 확률 ↑"),
    ("risk", "hy", "하이일드 스프레드", "fred", "BAMLH0A0HYM2", "level", "%p", "매일",
     "위험한 회사채 금리 − 국채 금리", "벌어지면 돈 빌리기 어려워짐 = 위험 신호 · 좁으면 안심"),
    ("risk", "nfci", "금융여건지수 (NFCI)", "fred", "NFCI", "level", "", "매주",
     "금리·신용·주가를 합친 시카고연은 지수", "0보다 높으면 긴축적(돈 줄 빡빡) · 낮으면 완화적"),
    ("risk", "gscpi", "공급망 압력 (GSCPI)", "nyfed", None, "level", "", "매월 초",
     "운임·배송 지연을 합친 뉴욕연은 공급망 지수", "0보다 높으면 공급망 막힘 → 물가 압력"),
    ("risk", "gold_copper", "금/구리 비율", "ratio", ("GC=F", "HG=F"), "level", "", "실시간",
     "안전자산(금) ÷ 경기 자산(구리)", "오르면 불안 > 경기 (위험 회피) · 내리면 경기 기대"),
    ("risk", "gold_silver", "금/은 비율", "ratio", ("GC=F", "SI=F"), "level", "", "실시간",
     "금 ÷ 은 (은은 절반이 산업용)", "높으면 불안 · 낮아지면 경기 회복 기대"),

    ("kr", "bok_rate", "한국 기준금리", "ecos", ("722Y001", "D", "0101000"), "level", "%", "금통위 연 8회",
     "한국은행이 정하는 원화 기준금리", "인상 = 긴축 · 인하 = 완화 · 연준과의 차이가 환율을 움직임"),
    ("kr", "kr_us_gap", "한미 금리차", "calc", None, "level", "%p", "수시",
     "한국 기준금리 − 미국 기준금리(상단)", "마이너스가 클수록 원화 약세·외국인 자금 유출 압력"),
    ("kr", "kr_cpi", "한국 CPI (전년비)", "ecos", ("901Y009", "M", "0"), "yoy", "%", "매월 초",
     "한국 소비자물가 상승률", "한국은행 금리 결정의 기준 · 2% 목표"),
    ("kr", "ktb3", "국고채 3년", "ecos", ("817Y002", "D", "010200000"), "level", "%", "매일",
     "한국 정부 3년 채권 금리", "한국은행 금리 예상이 담김 · 기준금리보다 낮으면 인하 기대"),
    ("kr", "cd91", "CD 91일", "ecos", ("817Y002", "D", "010502000"), "level", "%", "매일",
     "은행이 돈을 빌릴 때 발행하는 양도성예금증서 금리", "변동금리 대출의 기준 — 금리 경로 2단계"),
    ("kr", "fx_res", "외환보유액", "ecos", ("732Y001", "M", "99"), "scale:100000", "억달러", "매월 초",
     "한국이 가진 외화(달러 등) 총액", "환율 방어 여력 · 급감하면 원화 불안"),
    ("kr", "ca", "경상수지", "ecos", ("301Y013", "M", "000000"), "scale:100", "억달러", "매월 초",
     "한국이 수출·서비스·투자로 번 외화 − 쓴 외화", "흑자 = 원화 지지 · 적자 전환은 경고"),
    ("kr", "export", "한국 수출 (전년비)", "fred", "XTEXVA01KRM667S", "yoy", "%", "매월 (1~2달 늦음)",
     "한국 상품 수출액 증가율", "세계 경기 바로미터 · 빠른 숫자는 매월 1일 수출입 발표"),

    ("commod", "ng", "천연가스 (헨리허브)", "yahoo", "NG=F", "level", "달러", "실시간",
     "미국 천연가스 가격", "겨울 난방·전력 수요 · 오르면 에너지 물가 ↑"),
    ("commod", "rbob", "휘발유", "yahoo", "RB=F", "level", "달러", "실시간",
     "미국 휘발유 선물", "미국 소비자 물가·체감 경기에 직결"),
    ("commod", "ho", "경유·난방유", "yahoo", "HO=F", "level", "달러", "실시간",
     "트럭·배 연료 — 운송비의 핵심", "오르면 몇 주 뒤 운임 ↑ → 수입 물가 ↑"),
    ("commod", "alu", "알루미늄", "yahoo", "ALI=F", "level", "달러", "실시간",
     "자동차·포장 소재 (중국 생산 압도적)", "중국 생산·전력 사정 · 제조 원가"),
    ("commod", "plat", "백금", "yahoo", "PL=F", "level", "달러", "실시간",
     "자동차 배기 정화·수소 (남아공)", "산업 수요 + 귀금속 성격"),
    ("commod", "pall", "팔라듐", "yahoo", "PA=F", "level", "달러", "실시간",
     "자동차 배기 정화 (러시아 비중 큼)", "러시아 제재 뉴스에 민감"),
]
MACRO_GROUPS = {"fed": "🏛 연준 정책", "infl": "💹 물가", "job": "👷 고용", "growth": "📈 성장",
                "risk": "🚦 경기·위험 신호", "kr": "🇰🇷 한국 매크로", "commod": "🛢 에너지·금속 추가"}
_macro_ind_cache = {"at": 0.0, "data": None, "busy": False}


_macro_obs_cache: dict = {}  # 발표형 지표는 6시간 저장 (FRED·ECOS·뉴욕연은)


def _macro_cached(key: str, fn, ttl: int = 21600):
    c = _macro_obs_cache.get(key)
    if c and time.time() - c[0] < ttl and c[1]:
        return c[1]
    v = fn()
    if v:
        _macro_obs_cache[key] = (time.time(), v)
    return v or (c[1] if c else [])


def _fred_obs(sid: str, n: int = 30) -> list:
    return _macro_cached(f"fred:{sid}:{n}", lambda: _fred_obs_raw(sid, n))


def _fred_obs_raw(sid: str, n: int = 30) -> list:
    if not FRED_API_KEY:
        return []
    r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                     params={"series_id": sid, "api_key": FRED_API_KEY, "file_type": "json", "sort_order": "desc", "limit": n}, timeout=12)
    r.raise_for_status()
    return [(o["date"], float(o["value"])) for o in r.json().get("observations", []) if o.get("value") not in (".", None, "")]


def _ecos_obs(stat: str, cycle: str, item: str) -> list:
    return _macro_cached(f"ecos:{stat}:{item}", lambda: _ecos_obs_raw(stat, cycle, item))


def _ecos_obs_raw(stat: str, cycle: str, item: str) -> list:
    if not ECOS_API_KEY:
        return []
    now = now_kst()
    if cycle == "D":
        a, b = (now - timedelta(days=40)).strftime("%Y%m%d"), now.strftime("%Y%m%d")
    else:
        a, b = (now - timedelta(days=500)).strftime("%Y%m"), now.strftime("%Y%m")
    url = f"https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_API_KEY}/json/kr/1/200/{stat}/{cycle}/{a}/{b}/{item}"
    rows = (requests.get(url, timeout=12).json().get("StatisticSearch") or {}).get("row") or []
    out = []
    for x in rows:
        try:
            t = x["TIME"]
            d = f"{t[:4]}-{t[4:6]}-{t[6:8]}" if len(t) == 8 else f"{t[:4]}-{t[4:6]}-01"
            out.append((d, float(x["DATA_VALUE"])))
        except Exception:
            pass
    return sorted(out, reverse=True)


def _gscpi_obs() -> list:
    return _macro_cached("gscpi", _gscpi_obs_raw, 43200)


def _gscpi_obs_raw() -> list:
    try:
        import openpyxl
        r = requests.get("https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx", timeout=20)
        wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
        out = []
        for ws in wb.worksheets:
            for row in ws.iter_rows(values_only=True):
                if row and isinstance(row[0], datetime) and len(row) > 1 and isinstance(row[1], (int, float)):
                    out.append((row[0].strftime("%Y-%m-%d"), float(row[1])))
            if out:
                break
        return sorted(out, reverse=True)
    except Exception as e:
        print(f"[macro_ind] GSCPI 실패: {e}")
        return []


def _macro_fmt(v, unit):
    if v is None:
        return "—"
    if unit in ("%", "%p"):
        return f"{v:.2f}{unit}"
    if unit == "천명":
        return f"{v:+,.0f}천명"
    if unit in ("조달러",):
        return f"{v:,.2f}조달러"
    if abs(v) >= 1000:
        return f"{v:,.0f}" + (f" {unit}" if unit else "")
    return f"{v:,.2f}" + (f" {unit}" if unit else "")


def _macro_transform(obs: list, tf: str):
    """obs = [(date, value)] 최신순 → (값, 이전값, 날짜)"""
    if not obs:
        return None, None, None
    if tf == "level" or tf.startswith("scale:"):
        k = float(tf.split(":")[1]) if tf.startswith("scale:") else 1.0
        return obs[0][1] / k, (obs[1][1] / k if len(obs) > 1 else None), obs[0][0]
    if tf == "diff":
        if len(obs) < 3:
            return None, None, obs[0][0]
        return obs[0][1] - obs[1][1], obs[1][1] - obs[2][1], obs[0][0]
    if tf == "mom":
        if len(obs) < 3:
            return None, None, obs[0][0]
        return (obs[0][1] / obs[1][1] - 1) * 100, (obs[1][1] / obs[2][1] - 1) * 100, obs[0][0]
    if tf == "yoy":  # 월간: 12개 전, 분기면 4개 전
        lag = 12
        try:
            d0, d1 = datetime.strptime(obs[0][0], "%Y-%m-%d"), datetime.strptime(obs[1][0], "%Y-%m-%d")
            if (d0 - d1).days > 80:
                lag = 4
        except Exception:
            pass
        if len(obs) <= lag + 1:
            return None, None, obs[0][0]
        return (obs[0][1] / obs[lag][1] - 1) * 100, (obs[1][1] / obs[lag + 1][1] - 1) * 100, obs[0][0]
    return None, None, None


def _macro_ind_build() -> dict:
    out = []
    vals = {}
    for grp, key, name, src, sid, tf, unit, freq, meaning, read in MACRO_IND:
        v = pv = d = None
        note = ""
        try:
            if src == "fred":
                v, pv, d = _macro_transform(_fred_obs(sid, 30), tf)
            elif src == "fed_range":
                lo, hi = _fred_obs("DFEDTARL", 400), _fred_obs("DFEDTARU", 400)
                if lo and hi:
                    v, d = hi[0][1], hi[0][0]
                    prev_hi = next((x[1] for x in hi if x[1] != hi[0][1]), hi[0][1])
                    pv = prev_hi
                    note = f"{lo[0][1]:.2f}~{hi[0][1]:.2f}%"
            elif src == "ecos":
                if not ECOS_API_KEY:
                    note = "ECOS 키 필요"
                else:
                    v, pv, d = _macro_transform(_ecos_obs(*sid), tf)
            elif src == "nyfed":
                v, pv, d = _macro_transform(_gscpi_obs(), tf)
            elif src == "yahoo":
                q = _macro_quote(sid)
                if q.get("ok"):
                    v, pv, d = q["price"], q["price"] - q.get("change", 0), now_kst().strftime("%Y-%m-%d")
            elif src == "ratio":
                a, b = _macro_quote(sid[0]), _macro_quote(sid[1])
                if a.get("ok") and b.get("ok") and b["price"]:
                    v = a["price"] / b["price"]
                    pa, pb = a["price"] - a.get("change", 0), b["price"] - b.get("change", 0)
                    pv = pa / pb if pb else None
                    d = now_kst().strftime("%Y-%m-%d")
            elif src == "calc" and key == "kr_us_gap":
                if vals.get("bok_rate") is not None and vals.get("fed_rate") is not None:
                    v = vals["bok_rate"] - vals["fed_rate"]
                    d = now_kst().strftime("%Y-%m-%d")
                elif not ECOS_API_KEY:
                    note = "ECOS 키 필요"
        except Exception as e:
            note = f"받기 실패: {str(e)[:60]}"
        vals[key] = v
        out.append({"group": grp, "key": key, "name": name, "value": None if v is None else round(v, 4),
                    "prev": None if pv is None else round(pv, 4), "display": note if (key == "fed_rate" and note) else _macro_fmt(v, unit),
                    "prev_display": _macro_fmt(pv, unit) if pv is not None else "",
                    "chg": None if (v is None or pv is None) else round(v - pv, 4),
                    "date": d, "unit": unit, "freq": freq, "meaning": meaning, "read": read,
                    "note": "" if key == "fed_rate" else note, "source": src})
    return {"ok": True, "at": now_kst().strftime("%Y-%m-%d %H:%M"), "groups": MACRO_GROUPS, "items": out,
            "ecos": bool(ECOS_API_KEY)}


@app.get("/api/macro-indicators")
def macro_indicators_endpoint(refresh: bool = False):
    """🏛 매크로 1순위 지표 — 발표형은 6시간, 시세형(에너지·금속·비율)은 15분마다 새로"""
    c = _macro_ind_cache
    stale = (time.time() - c["at"]) > 900
    if (refresh or stale or not c["data"]) and not c["busy"]:
        c["busy"] = True

        def run():
            try:
                c["data"] = _macro_ind_build()
                c["at"] = time.time()
            except Exception as e:
                print(f"[macro_ind] 실패: {e}")
            finally:
                c["busy"] = False

        if c["data"] and not refresh:
            threading.Thread(target=run, daemon=True).start()  # 예전 값 먼저 주고 뒤에서 새로
        else:
            run()
    return c["data"] or {"ok": False, "error": "만드는 중 — 잠시 뒤 다시"}


@app.get("/api/macro-quotes")
def macro_quotes_endpoint(symbols: str):
    import concurrent.futures as cf
    syms = [x for x in symbols.split(",") if x][:60]
    _mq_symbols.update(syms)

    def one(sym):
        c = _mq_cache.get(sym)
        if c and time.time() - c[0] < 900:  # 미리 받아둔 값(15분 안)이면 바로
            return c[1]
        try:
            return _macro_quote(sym)
        except Exception as e:
            return {"symbol": sym, "ok": False, "error": str(e)}

    with cf.ThreadPoolExecutor(max_workers=8) as ex:  # 처음 보는 건 한꺼번에
        out = list(ex.map(one, syms))
    oldest = min((c[0] for sym in syms for c in [_mq_cache.get(sym)] if c), default=time.time())
    return {"ok": True, "quotes": out, "as_of": int(oldest), "age": int(time.time() - oldest)}


# ══════════════════════════════════════════════════════════════════════════
# 🏭 KRX 업종 시장 지도 — 네이버 테마 대신 거래소(KRX) 업종 분류로 (네이버는 자주 바뀌어서) (2026-10-28)
#   묶음 = KRX 업종, 크기 = 오늘 거래대금, 색 = 등락률. 장중엔 3분마다 서버가 미리 만들어 둠
# ══════════════════════════════════════════════════════════════════════════
_krx_map_cache = {"at": 0.0, "data": None}
_krx_map_lock = threading.Lock()


def _krx_sector_map_build(top: int = 14) -> dict:
    if stock is None:
        raise RuntimeError("pykrx 없음")
    day = now_kst().strftime("%Y%m%d")
    ohlcv = _krx_df(stock.get_market_ohlcv, day, market="ALL")
    if ohlcv is None or ohlcv.empty or float(ohlcv.get("거래대금", ohlcv.iloc[:, 0]).sum() or 0) == 0:
        day = _latest_trading_day()
        ohlcv = _krx_df(stock.get_market_ohlcv, day, market="ALL")
    sect = {}
    for mk in ("KOSPI", "KOSDAQ"):
        try:
            df = _krx_df(stock.get_market_sector_classifications, day, mk)
            for code, row in df.iterrows():
                sect[code] = (str(row.get("업종명") or "기타"), str(row.get("종목명") or code))
        except Exception as e:
            print(f"[krx_map] {mk} 업종 분류 실패: {e}")
    groups = {}
    for code, row in ohlcv.iterrows():
        if code not in sect:
            continue  # ETF·ETN·스팩 등 업종 분류 없는 건 빼기
        ind, name = sect[code]
        eok = float(row.get("거래대금") or 0) / 1e8
        if eok < 10:
            continue
        g = groups.setdefault(ind, {"theme": ind, "stocks": [], "money": 0.0, "up": 0.0})
        pct = float(row.get("등락률") or 0)
        g["stocks"].append({"name": name, "code": code, "pct": round(pct, 2), "eok": round(eok, 1)})
        g["money"] += eok
        if pct > 0:
            g["up"] += eok
    out = []
    for g in groups.values():
        st = sorted(g["stocks"], key=lambda x: -x["eok"])
        tot = sum(x["eok"] for x in st) or 1
        g["pct"] = round(sum(x["pct"] * x["eok"] for x in st) / tot, 2)  # 거래대금 가중 평균 등락률
        g["stocks"] = st[:10]
        g["money"] = round(g["money"], 1)
        out.append(g)
    out.sort(key=lambda g: -g["money"])
    for g in out:
        g.pop("up", None)
    return {"ok": True, "at": now_kst().strftime("%H:%M"), "date": f"{day[:4]}-{day[4:6]}-{day[6:]}", "source": "KRX",
            "themes": out[:top], "note": "KRX 업종 기준 · 크기 = 거래대금 · 3분마다 갱신"}


def _krx_map_refresh():
    if not _krx_map_lock.acquire(blocking=False):
        return
    try:
        d = _krx_sector_map_build()
        if d["themes"]:
            _krx_map_cache.update({"at": time.time(), "data": d})
    except Exception as e:
        print(f"[krx_map] 갱신 실패: {e}")
    finally:
        _krx_map_lock.release()


def _run_krx_map_job():
    now = now_kst()
    if now.weekday() >= 5 or not ((8, 58) <= (now.hour, now.minute) <= (15, 40)):
        return
    if time.time() - _krx_map_cache["at"] >= 170:
        _krx_map_refresh()


@app.get("/api/krx-sector-map")
def krx_sector_map_endpoint(refresh: bool = False):
    c = _krx_map_cache
    if c["data"] and not refresh:
        if time.time() - c["at"] >= 180:
            threading.Thread(target=_krx_map_refresh, daemon=True).start()
        return c["data"]
    _krx_map_refresh()
    return c["data"] or {"ok": False, "error": "KRX 업종 지도를 아직 못 만들었어요 (잠시 후 다시)"}


# ══════════════════════════════════════════════════════════════════════════
# 🆕 신규상장·검색 안 되는 종목 찾기 — 상장 당일엔 KRX 전체 목록(16시 갱신)에 아직 없어서 검색이 안 됐음
#   (예: 멜콘). 네이버 종목 자동완성으로 코드를 찾아 검색 목록(stock_fundamentals)에 바로 넣음 (2026-10-28)
# ══════════════════════════════════════════════════════════════════════════
def _lookup_stock_code(q: str) -> list:
    res = requests.get("https://ac.stock.naver.com/ac", params={"q": q, "target": "stock"},
                       headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
    items = (res.json() or {}).get("items") or []
    out = []
    for it in items:
        code, name = str(it.get("code") or ""), it.get("name") or ""
        nation = str(it.get("nationCode") or it.get("nation") or "KOR")
        if re.fullmatch(r"[0-9][0-9A-Z]{5}", code) and "KOR" in nation.upper():
            out.append({"code": code, "name": name})
    return out[:8]


def _add_to_search(rows: list):
    now = now_kst().isoformat()
    for r in rows:
        try:
            ex = supabase.table("stock_fundamentals").select("code").eq("code", r["code"]).limit(1).execute().data
            if not ex:
                supabase.table("stock_fundamentals").upsert({"code": r["code"], "name": r["name"], "synced_at": now}).execute()
        except Exception as e:
            print(f"[lookup] 검색 목록 추가 실패 {r}: {e}")


@app.get("/api/stock-lookup")
def stock_lookup_endpoint(q: str):
    """종목 검색에서 못 찾으면 화면이 이 주소로 물어봄 → 찾으면 검색 목록에도 넣어둠"""
    try:
        rows = _lookup_stock_code(q.strip())
        _add_to_search(rows)
        return {"ok": True, "items": rows}
    except Exception as e:
        return {"ok": False, "error": str(e), "items": []}


def _run_new_listing_job():
    """평일 08:20~08:50 — 오늘·이번 주 신규상장 종목을 미리 검색 목록에 넣어 둠 (종목분석 바로 가능하게)"""
    now = now_kst()
    if now.weekday() >= 5 or not ((7, 40) <= (now.hour, now.minute) <= (8, 50)):
        return
    key = now.strftime("%Y-%m-%d")
    if _brief_already_sent("job_new_listing", key):
        return
    try:
        lo, hi = (now - timedelta(days=10)).strftime("%Y-%m-%d"), (now + timedelta(days=7)).strftime("%Y-%m-%d")
        for x in fetch_kr_ipo_listings():
            if lo <= x["date"] <= hi:
                nm = re.sub(r"\(.*?\)", "", x["name"]).strip() or x["name"]  # '영광(구.영광공작소)' → '영광' (2026-11-02)
                hits = _lookup_stock_code(nm)
                exact = [h for h in hits if h["name"].replace(" ", "") == nm.replace(" ", "")] or hits[:1]
                _add_to_search(exact)
                time.sleep(0.2)
        _mark_brief_sent("job_new_listing", key)
    except Exception as e:
        print(f"[new_listing] 실패: {e}")


# ══════════════════════════════════════════════════════════════════════════
# 🦀 꽃게 검색기 (2026-10-28 JM HTS 조건식 그대로) — 작은 꽃게 / 큰 꽃게
#   대상: 전체(KRX) · 제외: 관리·우선주·증거금100%·거래정지·환기·정리매매·불성실공시·단기과열·ETF·ETN·스팩
#   작은 꽃게 = B and C and D and E
#   큰 꽃게   = A and B and C and D and E and F and G
#   A [일] 1봉전 26봉 이내 전일종가대비 종가 +15% 이상 1회 이상
#   B [일] 0봉전 5봉 이내 거래대금 ≥ CRAB_B_MIN_EOK (HTS 값 확인 필요)
#   C [일] 종가 > 5이평           D [일] 1봉전 고가 < 0봉전 종가        E [일] 시가 < 종가 (양봉)
#   F [5분] 1봉전 5봉 평균거래량 ≥ 50,000     G [5분] 0봉전 거래량 ≥ 1봉전 거래량 × 150%
#   ※ 하루치 기준(전일 고가·5이평 재료·26일 +15%·5일 거래대금)은 아침에 한 번 만들고, 장중엔 3분마다 오늘 시세로 판정
# ══════════════════════════════════════════════════════════════════════════
CRAB_B_MIN_EOK = 50.0  # HTS 조건 B: 5봉 이내 거래대금 5,000(백만원) = 50억 이상 1회 이상 (2026-10-28 확인)
CRAB_SKIP_NAME = re.compile(r"스팩|SPAC|ETN|\(전환\)|^(KODEX|TIGER|RISE|KBSTAR|ACE|SOL|HANARO|KOSEF|ARIRANG|PLUS|TIMEFOLIO|KIWOOM|KoAct|WON|BNK|UNICORN|TREX|FOCUS|마이티|파워|히어로즈|에셋플러스|VITA|1Q|TRUSTON)\b")
_crab_base = {"day": None, "data": None}
_crab_cache = {"at": 0.0, "data": None}
_crab_lock = threading.Lock()
_crab_bb = {}  # code → (day, bb300_upper)
_crab_seen: dict = {}  # 오늘 검색기별 처음 걸린 시각


def _crab_build_base() -> dict:
    """오늘 판정에 쓸 하루치 기준 — 최근 거래일 30일치 전종목 일봉 (KRX)"""
    today = now_kst().strftime("%Y-%m-%d")
    if _crab_base["day"] == today and _crab_base["data"]:
        return _crab_base["data"]
    days, d = [], now_kst() - timedelta(days=1)
    while len(days) < 27 and d > now_kst() - timedelta(days=60):
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    # 장 전에 만들어 DB에 둔 게 있으면 그걸 씀 (서버가 재시작돼도 다시 안 만듦)
    try:
        r = supabase.table("brief_cache").select("data").eq("key", "crab_base_v2").limit(1).execute().data
        if r and r[0]["data"].get("day") == today and r[0]["data"].get("base"):
            _crab_base.update({"day": today, "data": r[0]["data"]["base"]})
            return _crab_base["data"]
    except Exception:
        pass
    import concurrent.futures as cf

    def get_day(ymd):
        try:
            return ymd, _krx_df(stock.get_market_ohlcv, ymd, market="ALL")
        except Exception:
            return ymd, None

    got = dict(get_day(d) for d in days)  # KRX는 차례로 (몰아 보내면 KRX가 막음) — 하루 한 번이라 괜찮음
    hist = {}  # code → list of (date, open, high, close, value_eok, pct) 최신→과거
    for ymd in days:
        df = got.get(ymd)
        if df is None or df.empty or float(df["종가"].sum()) == 0:
            continue  # 휴장일
        for code, r in df.iterrows():
            hist.setdefault(code, []).append((ymd, float(r["시가"]), float(r["고가"]), float(r["종가"]), float(r.get("거래대금", 0)) / 1e8,
                                              float(r.get("등락률", 0))))
    # 종목 이름은 DB(검색 목록)에서 한 번에 — 예전엔 종목마다 KRX에 물어봐서(2,700번) 아주 느렸음
    names = {}
    try:
        for start in range(0, 6000, 1000):
            rows = supabase.table("stock_fundamentals").select("code,name").range(start, start + 999).execute().data or []
            for x in rows:
                names[x["code"]] = x["name"]
            if len(rows) < 1000:
                break
    except Exception:
        pass
    # 대상: 코스피·코스닥만 (HTS 업종 '전체'와 같게 — 코넥스 제외)
    allowed = set()
    try:
        for mk in ("KOSPI", "KOSDAQ"):
            allowed |= set(stock.get_market_ticker_list(days[0], market=mk))
    except Exception:
        allowed = set()
    base = {}
    for code, h in hist.items():
        if len(h) < 5:
            continue
        if allowed and code not in allowed:
            continue
        if not code.endswith("0"):
            continue  # 우선주(끝자리 5·7·9·K 등) — 이름이 아니라 종목코드로 정확하게
        name = names.get(code) or ""
        if name and CRAB_SKIP_NAME.search(name):
            continue
        prev = h[0]  # 1봉전(어제)
        closes4 = [x[3] for x in h[:4]]  # 5이평용 어제까지 4개
        jump15 = any(x[5] >= 15 for x in h[:26])  # A: 1봉전부터 26봉 이내 +15% 이상
        val4 = max([x[4] for x in h[:4]] or [0])  # B: 어제까지 4일 거래대금 최대 (오늘 것과 합쳐 5봉)
        base[code] = {"name": name, "prev_high": prev[2], "prev_close": prev[3], "closes4": closes4, "jump15": jump15, "val4": val4}
    _crab_base.update({"day": today, "data": base})
    try:
        supabase.table("brief_cache").upsert({"key": "crab_base_v2", "data": {"day": today, "base": base}, "updated_at": now_kst().isoformat()}).execute()
    except Exception:
        pass
    return base


_crab_status_cache: dict = {}  # code → (day, ok, why)


def _crab_status_ok(code: str):
    """HTS 제외종목과 같게: 관리·거래정지·증거금100%·단기과열·정리매매·투자유의(환기·불성실공시 등) 빼기
    — 한투 현재가의 종목 상태 칸으로 확인, 종목당 하루 한 번만 (2026-10-29)"""
    today = now_kst().strftime("%Y-%m-%d")
    c = _crab_status_cache.get(code)
    if c and c[0] == today:
        return c[1], c[2]
    ok, why = True, ""
    try:
        o = kis_get("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                    {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}).get("output") or {}
        stat = str(o.get("iscd_stat_cls_code") or "")
        checks = [
            (stat == "51" or str(o.get("mang_issu_cls_code") or "").upper() == "Y", "관리종목"),
            (stat == "58" or str(o.get("temp_stop_yn") or "").upper() == "Y", "거래정지"),
            (stat == "57" or float(str(o.get("marg_rate") or "0").replace(",", "") or 0) >= 100, "증거금100%"),
            (stat == "59" or str(o.get("short_over_yn") or "").upper() == "Y", "단기과열"),
            (str(o.get("sltr_yn") or "").upper() == "Y", "정리매매"),
            (str(o.get("invt_caful_yn") or "").upper() == "Y", "투자유의(환기·불성실공시 등)"),
        ]
        for bad, label in checks:
            if bad:
                ok, why = False, label
                break
    except Exception:
        ok, why = True, ""  # 확인 못 하면 일단 보여줌
    _crab_status_cache[code] = (today, ok, why)
    return ok, why


def _crab_bb300(code: str):
    """볼린저 300 상단 (차트의 정석 위치용) — 종목당 하루 한 번"""
    today = now_kst().strftime("%Y-%m-%d")
    c = _crab_bb.get(code)
    if c and c[0] == today:
        return c[1]
    try:
        # KRX 대신 한투 일봉으로 (KRX 부담 줄이기) — 100개씩 3번 이어 받기
        cl, end = [], now_kst().strftime("%Y%m%d")
        for _ in range(3):
            j = kis_get("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
                        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": "19900101",
                         "FID_INPUT_DATE_2": end, "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
            rows = j.get("output2") or []
            rows = [r for r in rows if r.get("stck_clpr")]
            if not rows:
                break
            cl = [float(r["stck_clpr"]) for r in rows][::-1] + cl
            oldest = min(r["stck_bsop_date"] for r in rows)
            end = (datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
            if len(rows) < 90:
                break
        cl = cl[-300:]
        if len(cl) < 120:
            _crab_bb[code] = (today, None)
            return None
        m = sum(cl) / len(cl)
        sd = (sum((x - m) ** 2 for x in cl) / len(cl)) ** 0.5
        up = m + 2 * sd
        _crab_bb[code] = (today, up)
        return up
    except Exception:
        return None


def _crab_position(price: float, ma5: float, bb: float) -> str:
    """차트의 정석 위치 (간이) — 볼린저 300 상단 · 5이평 기준"""
    if bb is None:
        return "—"
    gap = (price / bb - 1) * 100
    if gap >= 0:
        return "1번 돌파" if gap < 5 else "3번 진행"
    if gap > -5:
        return "1번 직전" if price >= ma5 else "2번 눌림"
    return "볼린저 아래"


def _crab_scan() -> dict:
    base = _crab_build_base()
    now = now_kst()
    snap = _krx_df(stock.get_market_ohlcv, now.strftime("%Y%m%d"), market="ALL")
    if snap is None or snap.empty or float(snap["종가"].sum()) == 0:
        return {"ok": False, "error": "오늘 시세가 아직 없어요 (장 시작 전·휴장)"}
    small, big = [], []
    for code, r in snap.iterrows():
        b = base.get(code)
        if not b:
            continue
        o, c, v = float(r["시가"]), float(r["종가"]), float(r.get("거래대금", 0)) / 1e8
        vol = float(r.get("거래량", 0))
        if o <= 0 or c <= 0:
            continue
        ma5 = (sum(b["closes4"]) + c) / 5
        B = max(b["val4"], v) >= CRAB_B_MIN_EOK
        C = c > ma5
        D = c > b["prev_high"]
        E = c > o
        if not (B and C and D and E):
            continue
        row = {"code": code, "name": b["name"] or code, "price": c, "pct": round((c / b["prev_close"] - 1) * 100, 2) if b["prev_close"] else 0,
               "value_eok": round(v, 1), "volume": int(vol), "ma5": ma5}
        small.append(row)
        if b["jump15"]:
            big.append(row)
    # 큰 꽃게 F·G (5분봉) — 후보만 확인
    import concurrent.futures as cf

    def fg(row):
        try:
            try:
                candles, *_ = fetch_minute_candles_paginated(row["code"], "J", "09:00")
            except Exception:
                time.sleep(1.2)  # 한투 초당 한도에 걸리면 잠깐 쉬고 한 번 더 (큰 꽃게가 안 나오던 원인)
                candles, *_ = fetch_minute_candles_paginated(row["code"], "J", "09:00")
            bars5 = {}
            for cd in candles:
                hh, mm = int(cd["time"][:2]), int(cd["time"][3:5])
                k = hh * 60 + (mm // 5) * 5
                bars5[k] = bars5.get(k, 0) + float(cd.get("volume", 0))
            vols = [bars5[k] for k in sorted(bars5)]
            if len(vols) >= 6:
                prev5 = vols[-6:-1]
                F = sum(prev5) / 5 >= 50000
                G = vols[-1] >= vols[-2] * 1.5 if vols[-2] > 0 else False
                if F and G:
                    return row
        except Exception:
            pass
        return None

    with cf.ThreadPoolExecutor(max_workers=2) as ex:  # 후보 2개씩 (분봉은 한 종목에 여러 번 물어봐서 많이 겹치면 한투가 막음)
        big_ok = [r for r in ex.map(fg, sorted(big, key=lambda x: -x["value_eok"])[:100]) if r]
    print(f"[crab] 큰 꽃게 후보 {len(big)}개 → 5분봉 통과 {len(big_ok)}개")
    # HTS 제외종목(관리·거래정지·증거금100%·단기과열·정리매매·투자유의) 빼기 — 후보만, 종목당 하루 한 번 확인
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        st = dict(zip([r["code"] for r in small], ex.map(lambda c: _crab_status_ok(c)[0], [r["code"] for r in small])))
    small = [r for r in small if st.get(r["code"], True)]
    big = [r for r in big if st.get(r["code"], True)]
    small = sorted(small, key=lambda x: -x["value_eok"])[:300]  # HTS는 100개 한계 — 우리는 다 보여줌 (안전상 300개까지)
    big_ok = sorted(big_ok, key=lambda x: -x["value_eok"])
    # 정석 위치(볼린저 300)는 종목마다 긴 일봉이 필요해서 거래대금 상위 25개만 (나머지는 "—") — 너무 오래 걸려 결과가 안 나오던 문제 (2026-10-28)
    # ⏱ 편입 시각: 오늘 그 검색기에 처음 걸린 시각 (조건시간 정렬용)
    day = now.strftime("%Y-%m-%d")
    if _crab_seen.get("day") != day:
        _crab_seen.clear()
        _crab_seen.update({"day": day, "small": {}, "big": {}})
    for key, lst in (("small", small), ("big", big_ok)):
        for row in lst:
            _crab_seen[key].setdefault(row["code"], now.strftime("%H:%M"))
    targets = {r["code"]: r for r in (big_ok + small[:15])}
    with cf.ThreadPoolExecutor(max_workers=5) as ex:
        bbs = dict(zip(targets.keys(), ex.map(_crab_bb300, targets.keys())))
    for row in big_ok + small:
        if row["code"] in bbs:
            row["position"] = _crab_position(row["price"], row["ma5"], bbs[row["code"]])
    for row in small + big_ok:
        row.pop("ma5", None)
    small = [{**r, "since": _crab_seen["small"].get(r["code"])} for r in small]
    big_ok = [{**r, "since": _crab_seen["big"].get(r["code"])} for r in big_ok]
    return {"ok": True, "day": now.strftime("%Y-%m-%d"), "at": now.strftime("%H:%M"), "small": small, "big": big_ok,
            "note": f"KRX 기준 · 3분마다 · 거래대금 5봉 이내 {CRAB_B_MIN_EOK:.0f}억↑"}


_crab_status = {"msg": "", "at": 0.0}


def _crab_refresh():
    if not _crab_lock.acquire(blocking=False):
        return
    t0 = time.time()
    try:
        _crab_status.update({"msg": "만드는 중", "at": t0})
        d = _crab_scan()
        if d.get("ok"):
            _crab_cache.update({"at": time.time(), "data": d})
            _crab_status["msg"] = ""
        else:
            _crab_status["msg"] = d.get("error", "실패")
        print(f"[crab] 검색 {'완료' if d.get('ok') else '실패'} {time.time() - t0:.0f}초 · 작은 {len(d.get('small', []))} 큰 {len(d.get('big', []))}")
    except Exception as e:
        _crab_status["msg"] = f"오류: {e}"
        print(f"[crab] 검색 실패: {e}")
    finally:
        _crab_lock.release()


def _run_crab_job():
    now = now_kst()
    if now.weekday() >= 5:
        return
    if (7, 50) <= (now.hour, now.minute) < (9, 0) and _crab_base["day"] != now.strftime("%Y-%m-%d"):  # 07:50부터 미리 (PC 원격 켜는 08:30 전에 끝나게)
        try:
            _crab_build_base()  # 장 전에 하루치 기준 미리
        except Exception as e:
            print(f"[crab] 기준 만들기 실패: {e}")
    if (9, 0) <= (now.hour, now.minute) <= (15, 35) and time.time() - _crab_cache["at"] >= 170:  # 09:00 장 시작부터
        _crab_refresh()


@app.get("/api/crab-screener")
def crab_screener_endpoint(refresh: bool = False):
    """결과가 있으면 바로, 없으면 뒤에서 만들기 시작하고 '만드는 중'을 바로 돌려줌 (화면이 10초마다 다시 물어봄)"""
    c = _crab_cache
    nowk = now_kst()
    if c["data"] and c["data"].get("day") != nowk.strftime("%Y-%m-%d") and nowk.weekday() < 5 and (nowk.hour, nowk.minute) >= (9, 0):
        c.update({"data": None, "at": 0.0})  # 장이 열리면 어제 결과는 지우고 오늘 것으로 (9시에 어제 종목이 보이지 않게)
    stale = not c["data"] or time.time() - c["at"] >= 180 or refresh
    failed_recently = (not c["data"] and _crab_status["msg"] and _crab_status["msg"] != "만드는 중"
                       and time.time() - _crab_status["at"] < 90)
    if failed_recently and not _crab_lock.locked():
        return {"ok": False, "error": f"🦀 {_crab_status['msg']}"}
    started = False
    if stale and not _crab_lock.locked():
        threading.Thread(target=_crab_refresh, daemon=True).start()
        started = True
    if c["data"] and not refresh:
        return c["data"]
    if _crab_lock.locked() or started:
        return {"ok": False, "building": True, "error": "🦀 검색 중… 처음엔 1~2분 걸려요 (자동으로 다시 확인)"}
    return {"ok": False, "error": _crab_status["msg"] or "검색 결과가 없어요"}


# 🖥 꽃게 검색기 PC(HTS) 결과 — PC 프로그램(crab_pc.py)이 키움 실시간 조건검색 결과를 올려줌
_crab_pc = {"data": None, "at": 0.0}
_crab_pc_view = {"at": 0.0, "data": None}


_crab_big_seen = {"day": "", "codes": set(), "top5": False, "pending": {}, "chk": 0.0}
_mkt_mood = {"at": 0.0, "weak": False, "txt": ""}


def _market_weak() -> tuple:
    """오늘 시장이 안 좋은가: 코스피·코스닥 중 하나라도 −1% 이하면 '약함' (1분 보관)"""
    m = _mkt_mood
    if time.time() - m["at"] < 60:
        return m["weak"], m["txt"]
    try:
        ks, kq = _macro_quote("^KS11")["changePct"], _macro_quote("^KQ11")["changePct"]
        m.update({"at": time.time(), "weak": min(ks, kq) <= -1.0, "txt": f"코스피 {ks:+.2f}% · 코스닥 {kq:+.2f}%"})
    except Exception:
        m["at"] = time.time()
    return m["weak"], m["txt"]


def _crab_big_alert(payload: dict):
    """🦀 큰 꽃게 알림 규칙 (2026-10-30 JM)
    ① 장 시작 후(09:03~) 한 번: 큰 꽃게 종목 중 거래대금 TOP 5
    ② 그 뒤 새로 편입: 거래대금 100억 이상만 (시장이 약한 날 = 코스피·코스닥 −1% 이하면 50억 이상)
       기준 아래로 편입된 종목은 대기 → 1분마다 다시 보고 기준 넘으면 그때 알림"""
    now = now_kst()
    day = now.strftime("%Y-%m-%d")
    st = _crab_big_seen
    if st["day"] != day:
        st.update({"day": day, "codes": set(), "top5": False, "pending": {}, "chk": 0.0})
    cur = {x["code"]: x for x in payload.get("big", []) or []}
    in_market = now.weekday() < 5 and (9, 0) <= (now.hour, now.minute) <= (15, 20)
    if not in_market:
        st["codes"] |= set(cur)
        return
    link = lambda c: f"🔗 https://jm-tracker.vercel.app/stock-analysis?code={c}"

    def q(code):
        try:
            return _kis_quote(code)
        except Exception:
            return {}

    # ① 장 시작 후 TOP 5 (한 번)
    if not st["top5"]:
        if (now.hour, now.minute) < (9, 3) or not cur:
            return  # 시초 몇 분은 기다렸다가 (시가 단일가 직후 숫자가 들쭉날쭉)
        import concurrent.futures as cf
        with cf.ThreadPoolExecutor(max_workers=4) as ex:
            qs = dict(zip(cur, ex.map(q, list(cur)[:40])))
        top = sorted(cur, key=lambda c: -(qs.get(c, {}).get("value_eok") or 0))[:5]
        weak, txt = _market_weak()
        lines = [f"🐻 곰발 장 초반 거래대금 TOP 5 ({now.strftime('%H:%M')} · 전체 {len(cur)}종목)", f"시장: {txt}{' 🔵 약함' if weak else ''}"]
        for i, c in enumerate(top, 1):
            v = qs.get(c, {})
            lines.append(f"{i}. {cur[c].get('name') or c} {int(v.get('price') or 0):,}원 ({(v.get('pct') or 0):+.2f}%) · {(v.get('value_eok') or 0):,.0f}억\n   {link(c)}")
        send_telegram("\n".join(lines))
        st["top5"] = True
        st["codes"] |= set(cur)
        return

    # ② 새 편입 → 거래대금 기준 넘는 것만
    weak, txt = _market_weak()
    th = 50 if weak else 100
    for c in cur:
        if c not in st["codes"]:
            st["codes"].add(c)
            st["pending"][c] = cur[c]
    for c in list(st["pending"]):
        if c not in cur:
            st["pending"].pop(c, None)  # 기준 넘기 전에 조건에서 빠지면 알림 안 함
    if not st["pending"] or time.time() - st["chk"] < 55:
        return
    st["chk"] = time.time()
    sent = 0
    for c, x in list(st["pending"].items()):
        v = q(c)
        val = v.get("value_eok") or 0
        if val >= th:
            send_telegram(f"🐻 곰발 편입 {x.get('since', now.strftime('%H:%M'))} · 거래대금 {val:,.0f}억 (기준 {th}억{' · 약한 장' if weak else ''})\n"
                          f"{x.get('name') or c} {int(v.get('price') or 0):,}원 ({(v.get('pct') or 0):+.2f}%)\n{link(c)}")
            st["pending"].pop(c, None)
            sent += 1
            if sent >= 5:
                break


def _gom_mark(code: str) -> str:
    """🐻 곰발 겹침 표시 (2026-11-09) — ' ●곰발' · ' ○통합' · ' 🎯' · '' (PC 곰발/곰발 통합 목록 + 서버 일봉 신호)"""
    d = _crab_pc.get("data") or {}
    g = any(x.get("code") == code for x in (d.get("big") or []))
    gi = any(x.get("code") == code for x in (d.get("gomt") or []))
    try:
        t = _jm_signal_series(code).get("today") or {}
        g = g or bool(t.get("g"))
        gi = gi or bool(t.get("gi"))
    except Exception:
        pass
    return " 🎯" if (g and gi) else " ●곰발" if g else " ○통합" if gi else ""


# ⚡ 단타 검색기 5종 키 (2026-11-07) — crab 큰형님 자전거 · small 완빨 · bong3 봉삼이 · gomt 곰발 통합 · big 곰발
CRAB_KEYS = ("crab", "small", "bong3", "gomt", "big")


# 🐻 오늘 곰발에 한 번이라도 잡힌 종목 (편입 + 이탈) — 다음 날 08:40 NXT 프리마켓에 씀 (2026-11-07)
_gom_day = {"day": "", "items": {}, "saved": 0}


def _gom_day_save(payload: dict):
    now = now_kst()
    if now.weekday() >= 5:
        return
    day = now.strftime("%Y-%m-%d")
    if _gom_day["day"] != day:
        _gom_day.update({"day": day, "items": {}, "saved": 0})
        try:  # 서버 재시작 대비 — 오늘 저장분 이어받기
            r = supabase.table("brief_cache").select("data").eq("key", f"gom_day:{day}").limit(1).execute().data
            if r:
                _gom_day["items"] = dict(r[0]["data"].get("items") or {})
        except Exception:
            pass
    n0 = len(_gom_day["items"])
    for k in ("big", "big_removed"):
        for x in payload.get(k, []) or []:
            if x.get("code"):
                _gom_day["items"].setdefault(x["code"], x.get("name") or x["code"])
    if len(_gom_day["items"]) != n0 or time.time() - _gom_day["saved"] > 600:
        _gom_day["saved"] = time.time()
        supabase.table("brief_cache").upsert({"key": f"gom_day:{day}", "data": {"day": day, "items": _gom_day["items"]},
                                              "updated_at": now.isoformat()}).execute()


@app.get("/api/gom-day")
def gom_day_status():
    """🐾 곰발 포착 저장 상태 확인 — 오늘 몇 종목 저장됐나 · 내일 08:40에 쓸 '어제' 목록"""
    today = now_kst().strftime("%Y-%m-%d")
    try:
        rows = supabase.table("brief_cache").select("key,data,updated_at").like("key", "gom_day:%").order("key", desc=True).limit(5).execute().data or []
    except Exception as e:
        return {"ok": False, "error": str(e)}
    d, items = _gom_prev_day_items()
    return {"ok": True, "today": today, "today_saved": len(_gom_day["items"]) if _gom_day["day"] == today else 0,
            "prev_day": d, "prev_items": items,
            "saved_days": [{"day": r["key"].split(":", 1)[1], "count": len((r["data"] or {}).get("items") or {}), "updated_at": r.get("updated_at")} for r in rows]}


def _gom_prev_day_items() -> tuple:
    """오늘 이전 가장 최근 거래일의 곰발 포착 종목 → (날짜, {code: name})"""
    today = now_kst().strftime("%Y-%m-%d")
    try:
        rows = supabase.table("brief_cache").select("key,data").like("key", "gom_day:%").order("key", desc=True).limit(5).execute().data or []
    except Exception:
        return "", {}
    for r in rows:
        d = r["key"].split(":", 1)[1]
        if d < today and (r["data"] or {}).get("items"):
            return d, dict(r["data"]["items"])
    return "", {}


_nxt_gom_info: dict = {"day": "", "rows": {}}


def _gom_snap_load(day: str) -> dict:
    try:
        r = supabase.table("brief_cache").select("data").eq("key", f"gom_snap:{day}").limit(1).execute().data
        return dict((r[0]["data"] or {}).get("rows") or {}) if r else {}
    except Exception:
        return {}


def _gom_snap_save(day: str, rows: dict):
    supabase.table("brief_cache").upsert({"key": f"gom_snap:{day}", "data": {"day": day, "rows": rows},
                                          "updated_at": now_kst().isoformat()}).execute()


def _gom_snapshot_job():
    """🐾 오늘 곰발 종목 — 정규장 마감 때 종가·등락률·거래대금·거래량(15:45) + 외국인·흑자/적자(18:10)를 저장해 두고
    다음 날 08:40 NXT 알림에 그대로 씀 (2026-11-10 JM: 아침에 하나하나 조회하면 시간 안에 못 끝냄)"""
    now = now_kst()
    if now.weekday() >= 5:
        return
    day = now.strftime("%Y-%m-%d")
    hm = (now.hour, now.minute)
    items = dict(_gom_day["items"]) if _gom_day["day"] == day else {}
    if not items:
        try:
            r = supabase.table("brief_cache").select("data").eq("key", f"gom_day:{day}").limit(1).execute().data
            items = dict((r[0]["data"] or {}).get("items") or {}) if r else {}
        except Exception:
            items = {}
    if not items:
        return

    def price_pass():
        rows = _gom_snap_load(day)
        for code in items:
            try:
                q = _kis_quote(code)  # 15:45 = 정규장 종가 (16시 애프터마켓 시작 전)
                if q.get("price"):
                    rows.setdefault(code, {}).update({"price": q["price"], "pct": q.get("pct", 0), "amount": (q.get("value_eok") or 0) * 1e8,
                                                      "volume": q.get("volume") or 0, "name": items[code]})
            except Exception:
                pass
            time.sleep(0.08)
        _gom_snap_save(day, rows)
        print(f"[gom_snap] 종가 저장 {len(rows)}종목")

    def extra_pass():
        rows = _gom_snap_load(day)
        for code in items:
            info = rows.setdefault(code, {"name": items[code]})
            try:
                inv = _investor_daily(code)
                if inv:
                    info["foreign"] = inv["foreign"]
            except Exception:
                pass
            try:
                q = fetch_dart_quarterly(code)
                info["status"] = q.get("latest_status") or ""
                qs = q.get("quarters") or []
                if qs and qs[-1].get("op") is not None:
                    info["op"] = qs[-1]["op"]
            except Exception:
                pass
            time.sleep(0.1)
        _gom_snap_save(day, rows)
        print(f"[gom_snap] 외국인·실적 저장 {len(rows)}종목")

    if (15, 45) <= hm < (16, 0) and not _brief_already_sent("job_gom_snap_px", day):
        _mark_brief_sent("job_gom_snap_px", day)
        threading.Thread(target=price_pass, daemon=True).start()
    if (18, 10) <= hm < (23, 0) and not _brief_already_sent("job_gom_snap_ex", day):
        _mark_brief_sent("job_gom_snap_ex", day)
        threading.Thread(target=extra_pass, daemon=True).start()


def _nxt_gom_prefetch():
    """08:20쯤 — 어제 곰발 종목의 외국인 순매수(전일) · 흑자/적자를 미리 받아 둠 (08:40 정각에 바로 보내려고)"""
    day = now_kst().strftime("%Y-%m-%d")
    pday, items = _gom_prev_day_items()
    snap = _gom_snap_load(pday) if pday else {}
    if snap and sum(1 for v in snap.values() if v.get("price")) >= len(items) * 0.8:
        _nxt_gom_info.update({"day": day, "rows": snap})  # 어제 저녁에 저장해 둔 걸 그대로
        return
    out = {}
    end = now_kst().strftime("%Y-%m-%d")
    start = (now_kst() - timedelta(days=12)).strftime("%Y-%m-%d")
    for code in list(items)[:40]:
        info = {}
        try:  # 어제 통합(KRX+NXT) 일봉 — 종가 · 등락률 · 거래대금 · 거래량 (NXT 프리마켓과 관계없음)
            cs = [c for c in _kis_daily_candles(code, start, end, "UN") if c["date"] < end]
            if len(cs) >= 2:
                y, p0 = cs[-1], cs[-2]
                info.update({"price": y["close"], "pct": (y["close"] / p0["close"] - 1) * 100 if p0["close"] else 0,
                             "amount": (y.get("amount") or y["close"] * y["volume"]), "volume": y["volume"], "date": y["date"]})
        except Exception:
            pass
        try:
            inv = _investor_daily(code)
            if inv:
                info["foreign"] = inv["foreign"]
        except Exception:
            pass
        try:
            q = fetch_dart_quarterly(code)
            info["status"] = q.get("latest_status") or ""
            qs = q.get("quarters") or []
            if qs and qs[-1].get("op") is not None:
                info["op"] = qs[-1]["op"]
        except Exception:
            pass
        out[code] = info
        time.sleep(0.1)
    _nxt_gom_info.update({"day": day, "rows": out})


@app.post("/api/crab-pc")
def crab_pc_post(payload: dict):
    # 🐻 곰발 편입 · TOP 5 알림은 끔 (2026-11-07 JM — 09:30 상승 랭킹에 곰발 표시로 대신)
    try:
        _gom_day_save(payload)
    except Exception as e:
        print(f"[gom_day] 저장 실패: {e}")
    _crab_pc.update({"data": payload, "at": time.time()})
    _crab_pc_view["at"] = 0  # 다음 조회 때 새로 시세 붙이기
    if time.time() - _crab_pc.get("db_at", 0) > 30:  # ⚡ PC가 3초마다 보내니까 DB 저장은 30초에 한 번만 (2026-11-07)
        _crab_pc["db_at"] = time.time()
        try:
            supabase.table("brief_cache").upsert({"key": "crab_pc", "data": payload, "updated_at": now_kst().isoformat()}).execute()
        except Exception:
            pass
    return {"ok": True}


# 🐻 곰발 · ▲ 역배정초 일봉 신호 (2026-11-02 JM) — 사이트 lib/jmSignals.ts와 같은 공식 (검색기에 '오늘 신호 있음/없음')
_sig_cache: dict = {}   # code → (계산 시각, {"g": bool, "y": bool})
_sig_busy = {"on": False}


_sig_series_cache: dict = {}  # code → (시각, 결과)


def _jm_signal_series(code: str) -> dict:
    """일봉 전체에서 🐻 곰발 · ▲ 역배정초가 뜬 날짜들 (차트 표시 + 검색기 '오늘 신호'에 같이 씀)"""
    c = _sig_series_cache.get(code)
    ttl = 600 if 9 <= now_kst().hour < 16 else 6 * 3600
    if c and time.time() - c[0] < ttl:
        return c[1]
    end = now_kst().strftime("%Y-%m-%d")
    # ▲ 역배정초는 볼린저 300이라 신호 날짜보다 300봉(약 15개월) 앞 데이터가 필요 → 약 4년치 받기
    #    (예전 760일이면 2025년 12월은 300봉이 안 돼서 계산 자체가 안 됐음 — 한미반도체 12월 미표시 원인, 2026-11-02)
    start = (now_kst() - timedelta(days=1460)).strftime("%Y-%m-%d")
    b = _kis_daily_candles(code, start, end)
    # 🟢 PC 켜져 있으면 키움 정규장 일봉으로 최근 구간 덮어쓰기 (한투 J는 밤에 애프터마켓 가격이 섞임) — 2026-11-07
    try:
        _kiwoom_daily_ask(code, "krx")
        kk = _kk_daily.get(code)
        if kk and kk[1]:
            mg = {x["date"]: x for x in b}
            for x in kk[1]:
                if start <= x["date"] <= end:
                    mg[x["date"]] = x
            b = [mg[k] for k in sorted(mg)]
    except Exception:
        pass
    n = len(b)
    C = [x["close"] for x in b]; H = [x["high"] for x in b]
    O = [x["open"] for x in b]
    A = [(x.get("amount") or 0) or x["close"] * x["volume"] for x in b]

    def sma_arr(arr, k):
        out, s_ = [None] * len(arr), 0.0
        for t in range(len(arr)):
            s_ += arr[t]
            if t >= k:
                s_ -= arr[t - k]
            if t >= k - 1:
                out[t] = s_ / k
        return out

    def bbu_arr(arr, k):
        m = sma_arr(arr, k)
        out = [None] * len(arr)
        for t in range(k - 1, len(arr)):
            w = arr[t - k + 1:t + 1]
            out[t] = m[t] + 2 * (sum((v - m[t]) ** 2 for v in w) / k) ** 0.5
        return out

    m5, m20, m60, a5 = sma_arr(C, 5), sma_arr(C, 20), sma_arr(C, 60), sma_arr(A, 5)
    u20 = bbu_arr(C, 20)
    u300 = bbu_arr(C, 300) if n >= 300 else [None] * n
    g_dates, y_dates, gi_dates = [], [], []
    CHG = [0.0] + [(C[j] / C[j - 1] - 1) * 100 if C[j - 1] else 0.0 for j in range(1, n)]
    OCHG = [(C[j] / O[j] - 1) * 100 if O[j] else 0.0 for j in range(n)]

    def gom_core(t):
        """🐻 곰발 공통 (2026-11-07 확정 · HTS 곰발/곰발 통합과 같은 A~K)"""
        if t < 40 or m5[t] is None:
            return False
        up8 = max(CHG[t - 25:t + 1]) >= 8
        today = C[t - 1] < C[t - 6] and (C[t] > C[t - 1] or C[t] > O[t - 1]) and up8      # 1봉전 5이평 하락 ⇔ C(1) < C(6)
        cont = C[t - 3] < C[t - 8] and (max(OCHG[t - 2:t + 1]) >= 3 or max(CHG[t - 2:t + 1]) >= 3) and up8  # 3봉전 5이평 하락
        base = C[t] > m5[t] and max(A[t - 4:t + 1]) >= 5e9 and max(A[t - 39:t + 1]) >= 5e10
        return (today or cont) and base

    def cu(t, line):
        return line[t] is not None and line[t - 1] is not None and H[t] > line[t] and H[t - 1] <= line[t - 1]

    for t in range(6, n):
        # 🐻 곰발 (●: 종가 > 전일 고가 · 양봉) / 곰발 통합 (○: 고가가 3봉 중 신고가) — 2026-11-07 확정
        if gom_core(t):
            if C[t] > H[t - 1] and C[t] > O[t]:
                g_dates.append(b[t]["date"])
            if H[t] >= max(H[t - 2:t + 1]):
                gi_dates.append(b[t]["date"])
        # ▲ 역배정초
        if t >= 305 and u300[t] is not None and u300[t - 2] is not None and m60[t] is not None and m20[t - 12] is not None:
            A1 = u300[t] > u300[t - 1] > u300[t - 2]
            A2 = m20[t - 10] < m20[t - 11] < m20[t - 12]
            tr = any(m20[t - k - 1] > m20[t - k] and m20[t - k + 1] > m20[t - k] for k in range(1, 6))
            A8 = any(cu(t - k, u20) for k in range(5))
            A11 = any(cu(t - k, m60) for k in range(5))
            if A1 and A2 and tr and A8 and m20[t] < m60[t] and C[t] > m20[t] and A11 and (a5[t] or 0) >= 5e9:
                y_dates.append(b[t]["date"])
    last = b[-1]["date"] if b else ""
    # 🦀 검색기 '일봉 신호' = 사부님 점 구조 (11/2 마감 16종목 비교로 확인): 종가 > 5이평 · 전일보다 상승 마감 · 양봉
    res = {"ok": True, "code": code, "bars": n, "g": g_dates, "gi": gi_dates, "y": y_dates,
           "today": {"g": bool(g_dates and g_dates[-1] == last), "gi": bool(gi_dates and gi_dates[-1] == last),
                     "y": bool(y_dates and y_dates[-1] == last)},
           "last": last, "source": "키움" if _kk_daily.get(code) else "한투"}
    _sig_series_cache[code] = (time.time(), res)
    return res


def _jm_signals_today(code: str) -> dict:
    return _jm_signal_series(code)["today"]


@app.get("/api/gom-debug")
def gom_debug(code: str, dates: str, market: str = "J"):
    """🐻 곰발 연구용 (2026-11-02): 날짜마다 곰발 조건 A~F 통과 여부 + '숨은 조건' 후보 숫자를 나란히
    예: /api/gom-debug?code=030530&dates=2026-08-10,2026-08-14,2026-09-02"""
    code = (code or "").strip().upper()
    want = [d.strip() for d in dates.split(",") if d.strip()]
    end = now_kst().strftime("%Y-%m-%d")
    start = (now_kst() - timedelta(days=760)).strftime("%Y-%m-%d")
    if market in ("KW", "KK"):  # 🟢 키움 일봉 (PC) — KW 통합 · KK 정규장
        st = _kk_daily if market == "KK" else _kw_daily
        b = (st.get(code) or (0, []))[1] or _kiwoom_daily_al([code], krx_only=(market == "KK")).get(code, [])
        if not b:
            return {"ok": True, "code": code, "rows": [{"date": d, "error": "키움 통합 일봉 없음 (PC 꺼짐?)"} for d in dates.split(",")]}
    else:
        b = _kis_daily_candles(code, start, end, market)  # J=KRX 정규장 · UN=통합(KRX+NXT, 장 전·장 후 포함)
    idx = {x["date"]: i for i, x in enumerate(b)}
    C = [x["close"] for x in b]; O = [x["open"] for x in b]; H = [x["high"] for x in b]; V = [x["volume"] for x in b]
    A = [(x.get("amount") or 0) or x["close"] * x["volume"] for x in b]
    sma = lambda arr, k, i: (sum(arr[i - k + 1:i + 1]) / k) if i >= k - 1 else None
    out = []
    for d in want:
        i = idx.get(d)
        if i is None or i < 30:
            out.append({"date": d, "error": "그날 일봉 없음(휴장일?)"})
            continue
        m5, m5p, m20, m60 = sma(C, 5, i), sma(C, 5, i - 1), sma(C, 20, i), sma(C, 60, i)
        surge = [j for j in range(max(1, i - 25), i + 1) if C[j] / C[j - 1] >= 1.10]
        conds = {
            "A_26봉내_+10%": bool(surge),
            "B_5봉내_거래대금50억": any(A[j] >= 5e9 for j in range(i - 4, i + 1)),
            "C_종가5이평돌파": bool(C[i] > m5 and C[i - 1] <= m5p),
            "D_전일고가돌파": H[i - 1] < C[i],
            "E_전일보다상승": C[i] > C[i - 1],
            "F_거래량증가": V[i] > V[i - 1],
        }
        hints = {
            "당일등락률%": round((C[i] / C[i - 1] - 1) * 100, 2),
            "양봉": C[i] > O[i],
            "당일거래대금억": round(A[i] / 1e8, 1),
            "거래량_전일대비배": round(V[i] / V[i - 1], 2) if V[i - 1] else None,
            "어제도_거래량증가였나": V[i - 1] > V[i - 2],   # false면 CrossUp(V, V(1)) = '증가로 전환한 날'
            "거래량_돌파전환(CrossUp)": V[i] > V[i - 1] and V[i - 1] <= V[i - 2],
            "거래량_5일평균대비배": round(V[i] / (sum(V[i - 5:i]) / 5), 2) if sum(V[i - 5:i]) else None,
            "5이평_상승중": m5 > m5p,
            "종가>20이평": C[i] > m20 if m20 else None,
            "5이평>20이평": m5 > m20 if m20 else None,
            "20이평>60이평": (m20 > m60) if (m20 and m60) else None,
            "A1봉전_오늘빼고_26봉내+10%": any(C[j] / C[j - 1] >= 1.10 for j in range(max(1, i - 26), i)),
            "A고가기준_26봉내+10%": any(H[j] / C[j - 1] >= 1.10 for j in range(max(1, i - 25), i + 1)),
            "A종가+8%_26봉내": any(C[j] / C[j - 1] >= 1.08 for j in range(max(1, i - 25), i + 1)),
            "B26봉내_거래대금50억": any(A[j] >= 5e9 for j in range(max(0, i - 25), i + 1)),
            "26봉내_최대등락%": round(max((C[j] / C[j - 1] - 1) * 100 for j in range(max(1, i - 25), i + 1)), 1),
            "26봉내_최대고가등락%": round(max((H[j] / C[j - 1] - 1) * 100 for j in range(max(1, i - 25), i + 1)), 1),
            "종가>5이평(돌파아니어도)": C[i] > m5,
            "5이평터치(저가≤5이평≤고가)": (b[i]["low"] <= m5 <= H[i]),   # 일봉 = 5이평 '터치' (11/3 JM 발견)
            "고가≥5이평": H[i] >= m5,
            "연속상승일수(오늘까지)": next((k for k in range(0, 15) if not (C[i - k] > C[i - k - 1])), 15),
            # 🐻 JM 가설 (11/4, 대원전선): 하락 2봉 → 5이평 접근 → 전일 몸통 상단(양봉=종가, 음봉=시가) 돌파
            "하락2봉(어제·그제 하락)": C[i - 1] < C[i - 2] < C[i - 3],
            "1봉전양봉": C[i - 1] > O[i - 1],                                  # 11/5 JM: 어제 캔들이 양봉
            "1봉전하락양봉": C[i - 1] > O[i - 1] and C[i - 1] < C[i - 2],      # 어제 하락 마감인데 양봉 (갭하락 후 매수)
            "2봉전양봉": C[i - 2] > O[i - 2],
            # 💡 11/5 아이디어: 어제 '매수세 흔적'을 양봉 말고 다르게 — 어제 종가가 캔들 어디쯤? / 어제 거래량이 말랐나?
            "1봉전종가위치%": round((C[i - 1] - b[i - 1]["low"]) / ((H[i - 1] - b[i - 1]["low"]) or 1) * 100),
            "1봉전거래량_5일평균배": round(V[i - 1] / ((sum(V[i - 6:i - 1]) / 5) or 1), 2),
            "오늘거래량_어제배": round(V[i] / (V[i - 1] or 1), 2),
            "5이평하락추세(어제<그제)": (sma(C, 5, i - 1) or 0) < (sma(C, 5, i - 2) or 0),
            "5이평이격도%": round(C[i] / m5 * 100, 1),
            # 💰 돈이 '꾸준히' 들어온 흔적 (11/4 JM: 기준봉 없이 연속 양봉 · 5이평 상승으로 수급 → 하락 → 곰발)
            "20봉내_최대연속양봉": max((sum(1 for _ in g) for k, g in __import__("itertools").groupby([C[j] > O[j] for j in range(i - 20, i)]) if k), default=0),
            "20봉내_5이평최대연속상승": max((sum(1 for _ in g) for k, g in __import__("itertools").groupby([(sma(C, 5, j) or 0) > (sma(C, 5, j - 1) or 0) for j in range(i - 20, i)]) if k), default=0),
            "20봉내_최대일거래대금억": round(max(A[i - 20:i]) / 1e8, 1),
            "5봉내_최대일거래대금억(오늘포함)": round(max(A[i - 4:i + 1]) / 1e8, 1),
            "고가이격도%": round(H[i] / m5 * 100, 1),
            "5이평하락_연속일(어제까지)": next((k for k in range(0, 30) if not ((sma(C, 5, i - 1 - k) or 0) < (sma(C, 5, i - 2 - k) or 0))), 30),
            "전일몸통상단": max(O[i - 1], C[i - 1]),
            "고가>전일몸통상단": H[i] > max(O[i - 1], C[i - 1]),
            "종가>전일몸통상단": C[i] > max(O[i - 1], C[i - 1]),
            "5이평근접(±2%)": abs(C[i] / m5 - 1) <= 0.02 or (b[i]["low"] <= m5 <= H[i]),
            # 🐻 JM (11/3): 일봉 '하락하다가 5이평 터치 → 상승 추세 몇 회'
            "5이평_연속상승일수": next((k for k in range(0, 15) if not ((sma(C, 5, i - k) or 0) > (sma(C, 5, i - k - 1) or 0))), 15),
            "터치_전_하락중(직전5일 5이평 하락)": sum(1 for k in range(1, 6) if (sma(C, 5, i - k) or 0) < (sma(C, 5, i - k - 1) or 0)) >= 3,
            "최근터치일로부터_며칠": next((k for k in range(0, 15) if (lambda mm: mm and b[i - k]["low"] <= mm <= H[i - k])(sma(C, 5, i - k))), None),
            "터치후_상승마감_횟수": (lambda t0: sum(1 for k in range(0, t0) if C[i - k] > C[i - k - 1]) if t0 is not None else None)(
                next((k for k in range(0, 15) if (lambda mm: mm and b[i - k]["low"] <= mm <= H[i - k])(sma(C, 5, i - k))), None)),
            "급등일로부터_며칠": (i - surge[-1]) if surge else None,
            "급등일": b[surge[-1]]["date"] if surge else None,
            "26봉최고가대비%": round((C[i] / max(H[i - 25:i + 1]) - 1) * 100, 1),
        }
        out.append({"date": d, "곰발_전부통과": all(conds.values()), "조건": conds, "숨은조건_후보": hints})
    return {"ok": True, "code": code, "rows": out}


def _gom_resolve(names: str, codes: str = "") -> tuple:
    lst = [c.strip().upper() for c in codes.split(",") if c.strip()]
    unresolved = []
    for nm in [x.strip() for x in names.split(",") if x.strip()]:
        code = None
        try:
            r = supabase.table("stock_fundamentals").select("code,name").eq("name", nm).limit(1).execute().data
            code = r[0]["code"] if r else None
        except Exception:
            pass
        if not code:
            try:
                hits = _lookup_stock_code(nm)
                ex = [h for h in hits if h["name"].replace(" ", "").upper() == nm.replace(" ", "").upper()] or hits[:1]
                code = ex[0]["code"] if ex else None
            except Exception:
                code = None
        (lst.append(code) if code else unresolved.append(nm))
    return list(dict.fromkeys(lst))[:40], unresolved


@app.get("/api/gom-check")
def gom_check(names: str = "", codes: str = "", date: str = "", j_eok: float = 50, k_eok: float = 500, d_pct: float = 8,
              premarket: bool = False, src: str = "krx", tong: int = 0):
    """🐻 곰발 확정식 A~N 조건별 체크 (2026-11-11) — 사부님 목록과 비교용. 키움 정규장 일봉 기준.
    예: /api/gom-check?names=이수페타시스,원익IPS   (date 생략 = 오늘)
        j_eok·k_eok·d_pct 로 J(5봉 거래대금)·K(40봉 거래대금)·D/H(26봉 급등%) 기준을 바꿔서 시험 가능"""
    d = (date or now_kst().strftime("%Y-%m-%d"))[:10]
    lst, unresolved = _gom_resolve(names, codes)
    src = "al" if tong else (src if src in _SRC_SUFFIX else "krx")  # 📏 기준가 출처 (2026-11-12): krx · al(통합) · nx
    sfx = _SRC_SUFFIX[src]
    store = _daily_store(sfx)
    try:
        _kiwoom_daily_al(lst, suffix=sfx)
    except Exception:
        pass
    rows = []
    start = (now_kst() - timedelta(days=200)).strftime("%Y-%m-%d")
    for code in lst:
        nm = code
        try:
            r = supabase.table("stock_fundamentals").select("name").eq("code", code).limit(1).execute().data
            nm = r[0]["name"] if r else code
        except Exception:
            pass
        try:
            b = _kis_daily_candles(code, start, d)
            kk = store.get(code)
            if kk and kk[1]:
                mg = {x["date"]: x for x in b}
                for x in kk[1]:
                    if start <= x["date"] <= d:
                        mg[x["date"]] = x
                b = [mg[k] for k in sorted(mg)]
            b = [x for x in b if x["date"] <= d]
            if premarket and b and b[-1]["date"] < d:
                # 🌅 장 전: 오늘 봉이 아직 없음 → HTS처럼 '납작한 오늘 봉'(시가=고가=저가=종가=어제 종가, 거래 0)을 붙여서 계산
                pc = b[-1]["close"]
                b = b + [{"date": d, "open": pc, "high": pc, "low": pc, "close": pc, "volume": 0, "amount": 0}]
            if not b or b[-1]["date"] != d or len(b) < 41:
                rows.append({"종목": nm, "오류": f"{d} 일봉 없음 (마지막 {b[-1]['date'] if b else '-'})"})
                continue
            t = len(b) - 1
            C = [x["close"] for x in b]; H = [x["high"] for x in b]; O = [x["open"] for x in b]
            A = [(x.get("amount") or 0) or x["close"] * x["volume"] for x in b]
            CHG = [0.0] + [(C[j] / C[j - 1] - 1) * 100 if C[j - 1] else 0.0 for j in range(1, len(b))]
            OCHG = [(C[j] / O[j] - 1) * 100 if O[j] else 0.0 for j in range(len(b))]
            m5 = sum(C[t - 4:t + 1]) / 5
            up = max(CHG[t - 25:t + 1])
            j5 = max(A[t - 4:t + 1]) / 1e8
            k40 = max(A[t - 39:t + 1]) / 1e8
            f = {
                "A 1봉전 5이평↓": C[t - 1] < C[t - 6],
                "B 종가>전일종가": C[t] > C[t - 1],
                "C 종가>전일시가": C[t] > O[t - 1],
                "D/H 26봉 급등": up >= d_pct,
                "E 3봉전 5이평↓": C[t - 3] < C[t - 8],
                "F 3봉내 시가대비3%": max(OCHG[t - 2:t + 1]) >= 3,
                "G 3봉내 전일대비3%": max(CHG[t - 2:t + 1]) >= 3,
                "I 종가>5이평": C[t] > m5,
                "J 5봉 거래대금": j5 >= j_eok,
                "K 40봉 거래대금": k40 >= k_eok,
                "L 종가>전일고가": C[t] > H[t - 1],
                "M 양봉": C[t] > O[t],
                "N 고가 3봉 신고가": H[t] >= max(H[t - 2:t + 1]),
                "M 5봉내 데드크로스": _dc5_ago(b, t) is not None,
            }
            today = f["A 1봉전 5이평↓"] and (f["B 종가>전일종가"] or f["C 종가>전일시가"]) and f["D/H 26봉 급등"]
            cont = f["E 3봉전 5이평↓"] and (f["F 3봉내 시가대비3%"] or f["G 3봉내 전일대비3%"]) and f["D/H 26봉 급등"]
            core = (today or cont) and f["I 종가>5이평"] and f["J 5봉 거래대금"] and f["K 40봉 거래대금"]
            blocked = []
            if not (today or cont):
                blocked.append("경로(A·B·C 오늘곰발 / E·F·G 연속곰발)" + ("" if f["D/H 26봉 급등"] else " + D/H"))
            for k in ("I 종가>5이평", "J 5봉 거래대금", "K 40봉 거래대금"):
                if not f[k]:
                    blocked.append(k)
            rows.append({"종목": nm, "코드": code, "출처": _SRC_LABEL[src] if (kk and kk[1]) else "한투 KRX(키움 못 받음)",
                         "등락%": round(CHG[t], 2), "종가": C[t],
                         "HTS곰발통합(실제식)": "O" if (today or cont) else "X",
                         "HTS실제식+K": "O" if ((today or cont) and f["K 40봉 거래대금"]) else "X",
                         "곰발통합(○)": "O" if core and f["N 고가 3봉 신고가"] else "X",
                         "곰발(●)": "O" if core and f["L 종가>전일고가"] and f["M 양봉"] else "X",
                         "곰발?(HTS식)": "O" if core and f["M 5봉내 데드크로스"] else "X",
                         "사부님식추정(M·I·D·K)": "O" if (f["M 5봉내 데드크로스"] and C[t] >= m5 and f["D/H 26봉 급등"] and f["K 40봉 거래대금"]) else "X",
                         "경로": "오늘곰발" if today else ("연속곰발" if cont else "-"),
                         "막힌조건": " · ".join(blocked) or ("N" if not f["N 고가 3봉 신고가"] else "-"),
                         **{k: ("O" if v else "X") for k, v in f.items()},
                         "26봉최대등락%": round(up, 1), "5봉최대대금억": round(j5, 1), "40봉최대대금억": round(k40, 1),
                         "이격도%": round(C[t] / m5 * 100, 1), "윗꼬리%": round((H[t] / max(C[t], O[t]) - 1) * 100, 2),
                         "5봉내 데드크로스": ("X" if _dc5_ago(b, t) is None else f"{_dc5_ago(b, t)}봉전"),
                         **_prev_candle(b, t),
                         **{k.replace("어제", "그제"): v for k, v in _prev_candle(b, t - 1).items()},
                         # 📏 장중 '선' (2026-11-12) — 현재가가 이 선을 넘나드는 순간 목록에 들어오고 빠짐 → 캡처 가격과 맞춰보기
                         "선_B 어제종가": C[t - 1], "선_C 어제시가": O[t - 1], "선_L 어제고가": H[t - 1],
                         "선_I 5이평돌파가(직전4일평균)": round(sum(C[t - 4:t]) / 4),
                         "선_N 직전2일 최고가": max(H[t - 2:t]), "오늘 고가": H[t],
                         # ⭐ 가설(11/12 11:00): 오늘 5이평이 아직 '하락 중'이어야 함 → 현재가 < 5일 전 종가 (넘으면 5이평이 위로 꺾여서 빠짐)
                         "선_U 5이평하락 상한(5일전종가)": C[t - 5],
                         "0봉 5이평하락": "O" if C[t] < C[t - 5] else "X"})
        except Exception as e:
            rows.append({"종목": nm, "오류": str(e)[:120]})
    tally = {}
    for r in rows:
        for k, v in r.items():
            if v == "X" and k[:1] in "ABCDEFGIJKLMN" and " " in k:
                tally[k] = tally.get(k, 0) + 1
    return {"ok": True, "date": d, "기준": {"J억": j_eok, "K억": k_eok, "D%": d_pct}, "조건별X개수": tally,
            "rows": rows, "못찾은종목": unresolved}


# 🐻 사부님 곰발바닥 마감 종목 5일치 (11/2~11/6, 53개) — 검증용 (2026-11-11)
MASTER_GOM_5D = {
    "2026-11-02": ["팬오션", "HD현대마린엔진", "한화오션", "코스맥스", "OCI홀딩스", "진코스텍", "효성중공업", "비아트론",
                   "한미사이언스", "리메드", "LG화학", "포스코퓨처엠"],
    "2026-11-03": ["매드업", "토마토시스템", "삼기에너지솔루션즈", "에스투더블유", "펩트론", "AP위성", "켄코아에어로스페이스",
                   "알테오젠", "한화머시너리앤서비스홀딩스"],
    "2026-11-04": ["국제약품", "동국산업", "코리아써키트", "한화솔루션", "삼표시멘트", "엘앤에프", "에코프로", "서진시스템", "심텍",
                   "에코프로비엠", "HD현대에너지솔루션", "두산퓨얼셀", "한선엔지니어링", "가온전선", "한국항공우주", "오킨스전자", "LS", "동진쎄미켐"],
    "2026-11-05": ["한국화장품제조", "GS건설", "JYP Ent.", "코스맥스", "엘앤씨바이오", "지투지바이오", "HD현대에너지솔루션", "대동기어", "대명에너지"],
    "2026-11-06": ["혜인", "GS건설", "엘리스", "JYP Ent.", "삼성에스디에스"],
}


def _prev_candle(b: list, t: int) -> dict:
    """🕯 1봉전(어제) 캔들 모양 — 사부님 종목 공통점 찾기용 (2026-11-11 JM: '전날 캔들에 답이 있다')"""
    y, p = b[t - 1], b[t - 2]
    o, h, l, c = y["open"], y["high"], y["low"], y["close"]
    rng = (h - l) or 1
    vols = [x["volume"] for x in b[t - 6:t - 1]]
    m5 = sum(x["close"] for x in b[t - 5:t]) / 5
    return {"어제 등락%": round((c / p["close"] - 1) * 100, 2) if p["close"] else 0,
            "어제 양음": "양봉" if c > o else ("음봉" if c < o else "도지"),
            "어제 몸통%": round(abs(c - o) / rng * 100), "어제 윗꼬리%": round((h - max(o, c)) / rng * 100),
            "어제 아래꼬리%": round((min(o, c) - l) / rng * 100), "어제 종가위치%": round((c - l) / rng * 100),
            "어제 시가갭%": round((o / p["close"] - 1) * 100, 2) if p["close"] else 0,
            "어제 거래량/5일평균": round(y["volume"] / (sum(vols) / len(vols)), 2) if vols and sum(vols) else None,
            "어제 종가/5이평%": round(c / m5 * 100, 1) if m5 else None,
            "어제 고가 위치": "5이평 위" if h > m5 else "5이평 아래"}


def _dc5_ago(b: list, t: int, win: int = 5):
    """✂️ 5봉 이내 1이평(종가)이 5이평을 아래로 뚫은 데드크로스 — 몇 봉 전이었나 (없으면 None) (2026-11-11 JM 아이디어)"""
    C = [x["close"] for x in b]
    ma = lambda j: sum(C[j - 4:j + 1]) / 5
    for k in range(0, win):
        j = t - k
        if j - 5 < 0:
            break
        if C[j - 1] >= ma(j - 1) and C[j] < ma(j):
            return k
    return None


def _gom_flags(b: list, t: int, a_win: int = 1, i_ge: bool = False, j_eok: float = 50, k_eok: float = 500, d_pct: float = 8,
               dc: str = "", simple: bool = False, hts: bool = False, hts_k: float = 0, hts_j: float = 0, hts_i: bool = False, low20: float = None, prev5: float = None) -> dict:
    """곰발 확정식 A~N (변형 옵션: A를 1~a_win봉전 중 하락 1회 · I를 ≥ 로 · dc=데드크로스 replace|and|or)"""
    C = [x["close"] for x in b]; H = [x["high"] for x in b]; O = [x["open"] for x in b]
    A = [(x.get("amount") or 0) or x["close"] * x["volume"] for x in b]
    CHG = lambda j: (C[j] / C[j - 1] - 1) * 100 if C[j - 1] else 0.0
    OCHG = lambda j: (C[j] / O[j] - 1) * 100 if O[j] else 0.0
    m5 = sum(C[t - 4:t + 1]) / 5
    up = max(CHG(j) for j in range(t - 25, t + 1))
    a_ok = any(C[t - k] < C[t - k - 5] for k in range(1, a_win + 1))
    if dc:
        d_ok = _dc5_ago(b, t) is not None
        a_ok = d_ok if dc == "replace" else (a_ok and d_ok) if dc == "and" else (a_ok or d_ok)
    today = a_ok and (C[t] > C[t - 1] or C[t] > O[t - 1]) and up >= d_pct
    cont = C[t - 3] < C[t - 8] and (max(OCHG(j) for j in range(t - 2, t + 1)) >= 3 or max(CHG(j) for j in range(t - 2, t + 1)) >= 3) and up >= d_pct
    i_ok = C[t] >= m5 if i_ge else C[t] > m5
    if simple:  # 🧪 사부님식 추정 (2026-11-12): 5봉내 데드크로스(눌림) + 오늘 5이평 위(회복) + 급등 이력 + 거래대금 — A·B·C·E·F·G 없이
        today = cont = (_dc5_ago(b, t) is not None) and up >= d_pct
    if hts:  # 🐻 HTS '곰발 통합' 실제 조건식 그대로 (2026-11-12): (A and (B or C) and D) or (E and (F or G) and H) — I·J·K·L은 체크만 되고 식에 없음
        core = (today or cont) and max(A[t - 39:t + 1]) >= hts_k * 1e8 and max(A[t - 4:t + 1]) >= hts_j * 1e8 and (i_ok or not hts_i)  # hts_i: I 5이평 시험 · hts_k·hts_j: 거래대금 거름망 시험용 (0 = 없음)
        if low20 is not None:  # 🔬 하방 컷: 종가가 20일 저가(오늘 제외)보다 low20% 이상 위 (2026-11-12 gom-diff)
            L = [x["low"] for x in b]
            core = core and C[t] >= min(L[t - 20:t]) * (1 + low20 / 100)
        if prev5 is not None:  # 🔬 이미 오름 컷: 어제까지 5일 수익률 ≤ prev5%
            core = core and (C[t - 1] / C[t - 6] - 1) * 100 <= prev5
        return {"core": core, "tong": core, "gom": core}
    core = (today or cont) and i_ok and max(A[t - 4:t + 1]) >= j_eok * 1e8 and max(A[t - 39:t + 1]) >= k_eok * 1e8
    return {"core": core, "tong": core and H[t] >= max(H[t - 2:t + 1]), "gom": core and C[t] > H[t - 1] and C[t] > O[t]}


# 📏 사부님 목록에서 읽은 '어제 종가' (11/12 화면 기준, 11/11 종가) — 어느 시세가 맞는지 대조용
MASTER_PREV_CLOSE = {"2026-11-12": {"파두": 36300, "우리로": 6850, "케이씨텍": 73400, "테크윙": 38800, "코리아써키트": 86400}}


@app.get("/api/gom-base")
def gom_base(names: str = "파두,우리로,케이씨텍,테크윙,코리아써키트", date: str = ""):
    """📏 기준가 대조 (2026-11-12): 같은 종목의 어제 봉을 KRX 정규장 · 통합 · NXT 세 가지로 받아 나란히 보여줌
    → 사부님 '어제 종가'가 어느 시세와 같은지 확인. 예: /api/gom-base?names=파두,우리로"""
    d = (date or now_kst().strftime("%Y-%m-%d"))[:10]
    lst, unresolved = _gom_resolve(names, "")
    got = {}
    for key, sfx in _SRC_SUFFIX.items():
        try:
            got[key] = _kiwoom_daily_al(lst, wait=60, suffix=sfx)
        except Exception:
            got[key] = {}
    known = MASTER_PREV_CLOSE.get(d, {})
    rows, hit = [], {k: 0 for k in _SRC_SUFFIX}
    for code in lst:
        nm = code
        try:
            r = supabase.table("stock_fundamentals").select("name").eq("code", code).limit(1).execute().data
            nm = r[0]["name"] if r else code
        except Exception:
            pass
        row = {"종목": nm, "코드": code}
        m = known.get(nm)
        if m:
            row["사부님 어제종가"] = m
        for key in _SRC_SUFFIX:
            b = [x for x in (got.get(key, {}).get(code) or []) if x["date"] <= d]
            lab = {"krx": "KRX", "al": "통합", "nx": "NXT"}[key]
            if len(b) < 2:
                row[f"{lab} 어제종가"] = "-"
                continue
            y, t = (b[-2], b[-1]) if b[-1]["date"] == d else (b[-1], None)
            row[f"{lab} 어제({y['date'][5:]}) 시가·고가·종가"] = f"{y['open']:,} · {y['high']:,} · {y['close']:,}"
            row[f"{lab} 어제종가"] = y["close"]
            if t:
                row[f"{lab} 오늘종가"] = t["close"]
            if m and y["close"] == m:
                hit[key] += 1
                row.setdefault("일치", [])
                row["일치"].append(lab)
        if m and "일치" not in row:
            row["일치"] = "없음"
        rows.append(row)
    pc = _pc_alive()
    return {"ok": True, "date": d, "PC연결": pc,
            "사부님과_일치_개수": {"KRX 정규장": hit["krx"], "통합": hit["al"], "NXT": hit["nx"], "비교 종목": len([r for r in rows if "사부님 어제종가" in r])},
            "rows": rows, "못찾은종목": unresolved,
            "설명": "어제종가가 사부님 값과 같은 칸 = 사부님이 쓰는 시세" + ("" if pc else " ⚠️ PC 꺼짐 — 키움 일봉 못 받음")}


# 🔬 사부님 흰색 vs 우리만 잡은 종목 — 일봉 지표로 둘을 가르는 '조건 X' 찾기 (2026-11-12)
GOM_DIFF_A = ("고영,금호건설,네오셈,대덕전자,대한광통신,덕산넵코어스,브이엠,비아트론,산일전기,삼성전자우,삼현,성호전자,심텍,양지사,"
              "이수스페셜티케미컬,이수페타시스,인텔리빅스,일진전기,지니언스,컨텍,케이씨텍,코리아써키트,티에스이,한빛레이저,"
              "효성중공업,AP위성,HD현대일렉트릭,LS ELECTRIC,RF머트리얼즈,SK")
GOM_DIFF_B = ("저스템,매드업,제이앤티씨,티엘비,한라캐스트,하이브,코스맥스,영광,삼양식품,큐리옥스바이오시스템즈,한선엔지니어링,SFA반도체,"
              "RFHIC,삼성전자,서진시스템,가온전선,비에이치,와이씨,세미파이브,대한전선,두산에너빌리티,필옵틱스,SK하이닉스,삼천당제약,"
              "동진쎄미켐,한국콜마,이오테크닉스,켄코아에어로스페이스,아모텍,유진테크,대원전선,원익홀딩스,한국화장품제조,에이비엘바이오,"
              "스카이랩스,실리콘투,달바글로벌")


def _gom_diff_feats(b: list, t: int) -> dict:
    C = [x["close"] for x in b]; H = [x["high"] for x in b]; L = [x["low"] for x in b]
    def ma(n, k=0):
        e = t - k + 1
        return sum(C[e - n:e]) / n if e - n >= 0 else None
    def pct(a, c):
        return round((a / c - 1) * 100, 1) if (a and c) else None
    f = {}
    for n in (5, 20, 60, 120):
        m = ma(n)
        f[f"종가/{n}이평%"] = pct(C[t], m)
    for n in (20, 60):
        m0, m5 = ma(n), ma(n, 5)
        f[f"{n}이평 5일기울기%"] = pct(m0, m5)
    for n in (20, 60, 120):
        if t - n >= 0:
            f[f"종가/{n}일고가%(오늘제외)"] = pct(C[t], max(H[t - n:t]))
            f[f"종가/{n}일저가%"] = pct(C[t], min(L[t - n:t]))
    for n in (5, 20, 60, 120):
        if t - n >= 0:
            f[f"{n}일 수익률%"] = pct(C[t], C[t - n])
    f["오늘 등락%"] = pct(C[t], C[t - 1])
    f["1봉전까지 5일 수익률%"] = pct(C[t - 1], C[t - 6]) if t >= 6 else None
    m5, m20, m60 = ma(5), ma(20), ma(60)
    f["정배열(5>20>60)"] = 1 if (m5 and m20 and m60 and m5 > m20 > m60) else 0
    f["20>60이평"] = 1 if (m20 and m60 and m20 > m60) else 0
    return f


@app.get("/api/gom-diff")
def gom_diff(a: str = GOM_DIFF_A, b: str = GOM_DIFF_B, date: str = ""):
    """🔬 A(사부님 흰색) vs B(우리만 잡은 종목) 일봉 지표 비교 — 지표마다 둘을 가장 잘 가르는 기준값과 정확도"""
    d = (date or now_kst().strftime("%Y-%m-%d"))[:10]
    start = (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=300)).strftime("%Y-%m-%d")
    groups, rows, miss = {"A": [], "B": []}, [], []
    for g, names in (("A", a), ("B", b)):
        nm_list = [x.strip() for x in names.split(",") if x.strip()]
        for i in range(0, len(nm_list), 40):
            lst, un = _gom_resolve(",".join(nm_list[i:i + 40]))
            miss += un
            for code in lst:
                try:
                    bars = [x for x in _kis_daily_candles(code, start, d) if x["date"] <= d]
                    if not bars or bars[-1]["date"] != d or len(bars) < 30:
                        miss.append(f"{code}(일봉부족)")
                        continue
                    nm = code
                    try:
                        r = supabase.table("stock_fundamentals").select("name").eq("code", code).limit(1).execute().data
                        nm = r[0]["name"] if r else code
                    except Exception:
                        pass
                    f = _gom_diff_feats(bars, len(bars) - 1)
                    groups[g].append(f)
                    rows.append({"그룹": "A 사부님흰색" if g == "A" else "B 우리만", "종목": nm, **f})
                except Exception as e:
                    miss.append(f"{code}({str(e)[:40]})")
    ranks = []
    keys = sorted({k for f in groups["A"] + groups["B"] for k in f})
    for k in keys:
        va = [f[k] for f in groups["A"] if f.get(k) is not None]
        vb = [f[k] for f in groups["B"] if f.get(k) is not None]
        if len(va) < 5 or len(vb) < 5:
            continue
        best = None
        for th in sorted(set(va + vb)):
            for side in (">=", "<="):
                pa = sum((v >= th) if side == ">=" else (v <= th) for v in va) / len(va)
                pb = sum((v >= th) if side == ">=" else (v <= th) for v in vb) / len(vb)
                score = (pa + (1 - pb)) / 2
                if not best or score > best[0]:
                    best = (score, th, side, pa, pb)
        sa, sb = sorted(va), sorted(vb)
        ranks.append({"지표": k, "가르는 정확도%": round(best[0] * 100), "기준": f"A면 {best[2]} {best[1]}",
                      "A 통과%": round(best[3] * 100), "B 통과%": round(best[4] * 100),
                      "A 중간값": sa[len(sa) // 2], "B 중간값": sb[len(sb) // 2]})
    ranks.sort(key=lambda r: -r["가르는 정확도%"])
    return {"ok": True, "date": d, "A개수": len(groups["A"]), "B개수": len(groups["B"]), "못찾음": miss,
            "지표 순위": ranks, "종목별": rows,
            "설명": "가르는 정확도 = (A 중 기준 통과 비율 + B 중 기준 탈락 비율) ÷ 2. 높을수록 사부님 흰색과 우리만 잡은 종목을 잘 가름"}


@app.get("/api/gom-validate")
def gom_validate(src: str = "krx", tong: int = 0, shift: int = 0):
    """🐻 사부님 5일치 마감 53개로 곰발 변형별 재현율 비교 — src: krx(정규장) · al(통합) · nx(NXT)"""
    src = "al" if tong else (src if src in _SRC_SUFFIX else "krx")
    sfx = _SRC_SUFFIX[src]
    store = _daily_store(sfx)
    allnames = sorted({n for v in MASTER_GOM_5D.values() for n in v})
    name_of = {}
    for nm in allnames:
        c, _ = _gom_resolve(nm)
        if c:
            name_of[nm] = c[0]
    try:
        _kiwoom_daily_al(list(name_of.values()), suffix=sfx)
    except Exception:
        pass
    variants = {
        "H0 HTS 곰발통합 그대로 (D 8%)": dict(hts=True),
        "H1 HTS + D 6%": dict(hts=True, d_pct=6),
        "H2 HTS + D 5%": dict(hts=True, d_pct=5),
        "H3 HTS + D 4.5%": dict(hts=True, d_pct=4.5),
        "H4 HTS + D 4%": dict(hts=True, d_pct=4),
        "H5 H3 + K 40봉 500억": dict(hts=True, d_pct=4.5, hts_k=500),
        "H6 H3 + K 500억 + J 10억": dict(hts=True, d_pct=4.5, hts_k=500, hts_j=10),
        "H7 H3 + K 300억": dict(hts=True, d_pct=4.5, hts_k=300),
        "H8 H3 + I 종가>5이평": dict(hts=True, d_pct=4.5, hts_i=True),
        "H9 H3 + I 종가≥5이평": dict(hts=True, d_pct=4.5, hts_i=True, i_ge=True),
        "H10 H5 + 20일저가 +10%↑": dict(hts=True, d_pct=4.5, hts_k=500, low20=10),
        "H11 H5 + 어제까지5일 ≤3.4%": dict(hts=True, d_pct=4.5, hts_k=500, prev5=3.4),
        "H12 H5 + 둘 다": dict(hts=True, d_pct=4.5, hts_k=500, low20=10, prev5=3.4),
        "H13 H5 + 20일저가 +10% + 5일 ≤5%": dict(hts=True, d_pct=4.5, hts_k=500, low20=10, prev5=5),
        "V0 지금 (J50·A1봉전)": dict(),
        "V1 J10억": dict(j_eok=10),
        "V2 J10억 + A 1~2봉전": dict(j_eok=10, a_win=2),
        "V3 V2 + I 종가≥5이평": dict(j_eok=10, a_win=2, i_ge=True),
        "V4 V3 + D 6%": dict(j_eok=10, a_win=2, i_ge=True, d_pct=6),
        "V5 V3 + A 1~3봉전": dict(j_eok=10, a_win=3, i_ge=True),
        "V6 A 대신 5봉내 데드크로스": dict(j_eok=10, i_ge=True, dc="replace"),
        "V7 A 또는 5봉내 데드크로스": dict(j_eok=10, i_ge=True, dc="or"),
        "V8 A 그리고 5봉내 데드크로스": dict(j_eok=10, i_ge=True, dc="and"),
        "V9 사부님식 추정 (M·I·D·K만)": dict(j_eok=0, i_ge=True, simple=True),
        "V10 V9 + J10억": dict(j_eok=10, i_ge=True, simple=True),
        "V11 V9 + D 5%": dict(j_eok=0, i_ge=True, simple=True, d_pct=5),
    }
    bars_cache = {}
    res = {k: {"통합(○)": 0, "곰발(●)": 0, "기본(N전)": 0, "놓침(○)": []} for k in variants}
    total, nodata, feats = 0, [], []
    for d, names in MASTER_GOM_5D.items():
        for nm in names:
            code = name_of.get(nm)
            if not code:
                nodata.append(f"{d} {nm}(코드 못찾음)")
                continue
            if code not in bars_cache:
                start = (datetime.strptime("2026-11-02", "%Y-%m-%d") - timedelta(days=200)).strftime("%Y-%m-%d")
                b = _kis_daily_candles(code, start, now_kst().strftime("%Y-%m-%d"))
                kk = store.get(code)
                if kk and kk[1]:
                    mg = {x["date"]: x for x in b}
                    for x in kk[1]:
                        if x["date"] >= start:
                            mg[x["date"]] = x
                    b = [mg[k] for k in sorted(mg)]
                bars_cache[code] = b
            b = bars_cache[code]
            idx = next((i for i, x in enumerate(b) if x["date"] == d), None)
            if idx is not None:
                idx += shift  # 🔀 2026-11-12 JM 발견: 사부님 d일 목록 vs 우리 식 d+shift일 계산 (하루 밀림 확인용)
                if idx >= len(b):
                    idx = None
            if idx is None or idx < 45:
                nodata.append(f"{d} {nm}(일봉 없음)")
                continue
            total += 1
            dk = _dc5_ago(b, idx)
            feats.append({"날짜": d[5:], "종목": nm, "5봉내 데드크로스": "X" if dk is None else f"{dk}봉전", **_prev_candle(b, idx)})
            for k, opt in variants.items():
                f = _gom_flags(b, idx, **opt)
                res[k]["통합(○)"] += f["tong"]
                res[k]["곰발(●)"] += f["gom"]
                res[k]["기본(N전)"] += f["core"]
                if not f["tong"]:
                    res[k]["놓침(○)"].append(f"{d[5:]} {nm}")
    for k in res:
        for kk_ in ("통합(○)", "곰발(●)", "기본(N전)"):
            res[k][kk_] = f"{res[k][kk_]}/{total} ({res[k][kk_] / total * 100:.0f}%)" if total else "0"
    # 🕯 사부님 53개의 어제 캔들 모양 요약
    def cnt(key, val):
        return sum(1 for f in feats if f.get(key) == val)
    summ = {"어제 양봉": cnt("어제 양음", "양봉"), "어제 음봉": cnt("어제 양음", "음봉"), "어제 도지": cnt("어제 양음", "도지"),
            "어제 하락 마감": sum(1 for f in feats if f["어제 등락%"] < 0),
            "어제 종가 5이평 아래": sum(1 for f in feats if (f["어제 종가/5이평%"] or 0) < 100),
            "어제 아래꼬리 30%↑": sum(1 for f in feats if f["어제 아래꼬리%"] >= 30),
            "어제 윗꼬리 30%↑": sum(1 for f in feats if f["어제 윗꼬리%"] >= 30),
            "어제 거래량 5일평균 미만": sum(1 for f in feats if (f["어제 거래량/5일평균"] or 0) < 1),
            "5봉내 데드크로스 있음": sum(1 for f in feats if f["5봉내 데드크로스"] != "X")}
    return {"ok": True, "출처": _SRC_LABEL[src], "shift": f"사부님 d일 목록 ↔ 우리 식 d{shift:+d}일" if shift else "같은 날", "종목수": total, "데이터없음": nodata, "결과": res, "어제캔들_요약": summ, "어제캔들": feats,
            "설명": "통합(○)=곰발 통합 · 곰발(●) · 기본(N전)=마무리 조건 빼고 A~K만. 사부님 마감 종목을 몇 개 잡나(재현율)"}


@app.get("/api/gom-debug-multi")
def gom_debug_multi(codes: str = "", date: str = "", names: str = "", market: str = "J"):
    """🐻 곰발 연구 — 여러 종목 한 번에 (한 줄씩 요약).
    예: /api/gom-debug-multi?date=2026-11-02&codes=008930,302550
        /api/gom-debug-multi?date=2026-11-03&names=현대건설,리메드   ← 종목명으로도 (2026-11-03)"""
    d = (date or now_kst().strftime("%Y-%m-%d"))[:10]
    lst = [c.strip().upper() for c in codes.split(",") if c.strip()]
    unresolved = []
    for nm in [x.strip() for x in names.split(",") if x.strip()]:
        code = None
        try:
            r = supabase.table("stock_fundamentals").select("code,name").eq("name", nm).limit(1).execute().data
            code = r[0]["code"] if r else None
        except Exception:
            pass
        if not code:
            try:
                hits = _lookup_stock_code(nm)
                ex = [h for h in hits if h["name"].replace(" ", "") == nm.replace(" ", "")] or hits[:1]
                code = ex[0]["code"] if ex else None
            except Exception:
                code = None
        if code:
            lst.append(code)
        else:
            unresolved.append(nm)
    lst = list(dict.fromkeys(lst))[:40]
    if market in ("KW", "KK"):
        _kiwoom_daily_al(lst, krx_only=(market == "KK"))  # 한 번에 맡기기
    names = {}
    try:
        for r in supabase.table("stock_fundamentals").select("code,name").in_("code", lst).execute().data or []:
            names[r["code"]] = r["name"]
    except Exception:
        pass
    rows = []
    for c in lst:
        try:
            r = gom_debug(c, d, market)["rows"][0]
            if r.get("error"):
                rows.append({"종목": names.get(c, c), "오류": r["error"]})
                continue
            k, h = r["조건"], r["숨은조건_후보"]
            yn = lambda v: "O" if v else "X"
            rows.append({
                "종목": names.get(c, c),
                "A0봉전+10%": yn(k["A_26봉내_+10%"]), "A1봉전(오늘빼고)": yn(h["A1봉전_오늘빼고_26봉내+10%"]),
                "B대금50억": yn(k["B_5봉내_거래대금50억"]), "C돌파": yn(k["C_종가5이평돌파"]), "C위": yn(h["종가>5이평(돌파아니어도)"]),
                "D고가돌파": yn(k["D_전일고가돌파"]), "E상승마감": yn(k["E_전일보다상승"]), "F거래량증가": yn(k["F_거래량증가"]),
                "F돌파전환": yn(h["거래량_돌파전환(CrossUp)"]), "양봉": yn(h["양봉"]),
                "등락%": h["당일등락률%"], "대금억": h["당일거래대금억"], "거래량5일배": h["거래량_5일평균대비배"],
                "5이평↑": yn(h["5이평_상승중"]), "종가>20": yn(h["종가>20이평"]), "20>60": yn(h["20이평>60이평"]),
                "급등후일": h["급등일로부터_며칠"], "최고가대비%": h["26봉최고가대비%"],
                "5이평터치": yn(h["5이평터치(저가≤5이평≤고가)"]), "고가≥5이평": yn(h["고가≥5이평"]),
                "연속상승일": h["연속상승일수(오늘까지)"],
                "하락2봉": yn(h["하락2봉(어제·그제 하락)"]), "1봉전양봉": yn(h["1봉전양봉"]), "1봉전종가위치%": h["1봉전종가위치%"], "1봉전거래량_5일평균배": h["1봉전거래량_5일평균배"], "오늘거래량_어제배": h["오늘거래량_어제배"], "1봉전하락양봉": yn(h["1봉전하락양봉"]), "2봉전양봉": yn(h["2봉전양봉"]), "5이평하락추세": yn(h["5이평하락추세(어제<그제)"]),
                "이격도%": h["5이평이격도%"], "20봉_최대연속양봉": h["20봉내_최대연속양봉"],
                "20봉_5이평최대연속상승": h["20봉내_5이평최대연속상승"], "20봉_최대일거래대금억": h["20봉내_최대일거래대금억"], "5봉_최대일거래대금억": h["5봉내_최대일거래대금억(오늘포함)"], "고가이격도%": h["고가이격도%"], "5이평하락연속일": h["5이평하락_연속일(어제까지)"], "고가>전일몸통상단": yn(h["고가>전일몸통상단"]),
                "종가>전일몸통상단": yn(h["종가>전일몸통상단"]), "5이평근접": yn(h["5이평근접(±2%)"]),
                "5이평연속상승일": h["5이평_연속상승일수"], "터치전하락중": yn(h["터치_전_하락중(직전5일 5이평 하락)"]),
                "터치후며칠": h["최근터치일로부터_며칠"], "터치후상승마감수": h["터치후_상승마감_횟수"],
                "A고가+10%": yn(h["A고가기준_26봉내+10%"]), "A종가+8%": yn(h["A종가+8%_26봉내"]), "B26봉50억": yn(h["B26봉내_거래대금50억"]),
                "26봉최대등락%": h["26봉내_최대등락%"], "26봉최대고가등락%": h["26봉내_최대고가등락%"],
            })
        except Exception as e:
            rows.append({"종목": names.get(c, c), "오류": str(e)})
        time.sleep(0.1)
    return {"ok": True, "date": d, "market": market, "rows": rows, "못찾은종목": unresolved}


def _gom_minute_features(code: str, d: str) -> dict:
    """🐻 분봉 연구 (2026-11-03): 그날 3분봉·5분봉에서 사부님 '1번·3번 자리' 후보 신호가 났는지, 몇 시에 났는지"""
    # 그날 + 앞 2~3거래일 1분봉 (볼린저 300을 3분봉으로 계산하려면 약 900분 = 2.5일치 필요)
    days, dt = [], datetime.strptime(d, "%Y-%m-%d")
    while len(days) < 4:
        if dt.weekday() < 5:
            days.append(dt.strftime("%Y-%m-%d"))
        dt -= timedelta(days=1)
    days = days[::-1]
    raw = []
    for dd in days:
        try:
            cs = (_minute_day_cached(code, dd) or {}).get("candles") or []
        except Exception:
            cs = []
        raw += [(dd, c) for c in cs]

    def agg(tf):
        bars, order = {}, []
        for dd, c in raw:
            hh, mm = int(c["time"][:2]), int(c["time"][3:5])
            if (hh, mm) < (9, 0) or (hh, mm) > (15, 30):
                continue
            k = (dd, hh * 60 + (mm // tf) * tf)
            b = bars.get(k)
            if not b:
                bars[k] = {"day": dd, "t": f"{k[1] // 60:02d}:{k[1] % 60:02d}", "o": c["open"], "h": c["high"], "l": c["low"], "c": c["close"], "v": c.get("volume", 0)}
                order.append(k)
            else:
                b["h"] = max(b["h"], c["high"]); b["l"] = min(b["l"], c["low"]); b["c"] = c["close"]; b["v"] += c.get("volume", 0)
        return [bars[k] for k in sorted(order)]

    def bbu(arr, n, i):
        if i < n - 1:
            return None
        w = arr[i - n + 1:i + 1]
        m = sum(w) / n
        return m + 2 * (sum((x - m) ** 2 for x in w) / n) ** 0.5

    out = {}
    for tf in (3, 5):
        bs = agg(tf)
        C = [b["c"] for b in bs]; V = [b["v"] for b in bs]
        idx = [i for i, b in enumerate(bs) if b["day"] == d]
        cross, near, vol_brk, touch300, body300 = [], [], [], [], []
        for i in idx:
            u, up = bbu(C, 300, i), bbu(C, 300, i - 1)
            if u and bs[i]["h"] >= u:  # 🐻 빨간선 '터치'만 해도 1번 (11/3 사부님 대화) — 고가가 빨간선에 닿음
                touch300.append(bs[i]["t"])
            if u and max(bs[i]["o"], C[i]) >= u:  # ☆ 11/4 JM: 근접은 '몸통'이 터치해야 (꼬리는 페이크)
                body300.append(bs[i]["t"])
            if u and up:
                if C[i] > u and C[i - 1] <= up:
                    u30 = bbu(C, 300, i - 30)
                    slope = round((up / u30 - 1) * 100, 2) if u30 else None
                    day_idx = [j for j in idx if j <= i]
                    cum_amt = sum(C[j] * V[j] for j in day_idx) / 1e8
                    avg20v = (sum(V[max(0, i - 20):i]) / 20) if i >= 1 else 0
                    cross.append({"시각": bs[i]["t"], "빨간선_30봉기울기%": slope,
                                  "자리": ("1번(하락 후 돌파)" if slope is not None and slope < -0.3 else "3번(완만한 돌파)"),
                                  "돌파봉_거래량": int(V[i]), "돌파봉_거래량_20봉평균배": round(V[i] / avg20v, 2) if avg20v else None,
                                  "직전봉보다_거래량↑": V[i] > V[i - 1],
                                  "돌파봉_거래대금억": round(C[i] * V[i] / 1e8, 1), "돌파시점_누적거래대금억": round(cum_amt, 1)})
                elif bs[i]["h"] >= u * 0.995 and C[i] < u:
                    near.append(bs[i]["t"])
            prevmax = max(V[max(0, i - 20):i]) if i > 0 else 0
            hi20 = max(b["h"] for b in bs[max(0, i - 20):i]) if i > 0 else 0
            if prevmax and V[i] > prevmax * 1.5 and C[i] > hi20:
                vol_brk.append(bs[i]["t"])
        # 🐻 JM 가설 (11/3): 분봉 5이평 터치 → 상승 n회 · 전일 종가 첫 도달(1번) → 눌림 → 1번 고점 재돌파(3번)
        extra = {}
        if tf == 3 and idx:
            prev = [b for b in bs if b["day"] < d]
            pc = prev[-1]["c"] if prev else None
            m5 = lambda j: (sum(C[j - 4:j + 1]) / 5) if j >= 4 else None
            touch = next((j for j in idx if m5(j) and bs[j]["l"] <= m5(j) <= bs[j]["h"]), None)
            streak_best, streak_time, cur = 0, None, 0
            if touch is not None:
                for j in [k for k in idx if k > touch]:
                    cur = cur + 1 if C[j] > C[j - 1] else 0
                    if cur > streak_best:
                        streak_best, streak_time = cur, bs[j]["t"]
            cumv = lambda j: round(sum(C[k] * V[k] for k in idx if k <= j) / 1e8, 1)
            vmul = lambda j: round(V[j] / (sum(V[max(0, j - 20):j]) / 20), 2) if j >= 1 and sum(V[max(0, j - 20):j]) else None
            one = next((j for j in idx if pc and bs[j]["h"] >= pc), None)
            three, dip = None, None
            if one is not None:
                peak, low = bs[one]["h"], bs[one]["l"]
                pulled = False
                for j in [k for k in idx if k > one]:
                    if not pulled:
                        peak = max(peak, bs[j]["h"])
                        if bs[j]["l"] <= peak * 0.99:
                            pulled, low = True, bs[j]["l"]
                    else:
                        low = min(low, bs[j]["l"])
                        if C[j] > peak:
                            three = j
                            break
                dip = round((low / peak - 1) * 100, 2) if pulled else None
            prevday = [b for b in bs if b["day"] == (prev[-1]["day"] if prev else None)]
            body_top = max(prevday[0]["o"], prevday[-1]["c"]) if prevday else None
            bt = next((j for j in idx if body_top and bs[j]["h"] > body_top), None)
            extra = {
                "전일몸통상단": body_top,
                "전일몸통상단_첫돌파": bs[bt]["t"] if bt is not None else None,
                "전일종가": pc,
                "5이평첫터치": bs[touch]["t"] if touch is not None else None,
                "터치후_최대연속상승봉": streak_best, "연속상승_최대시각": streak_time,
                "1번_전일종가첫도달": ({"시각": bs[one]["t"], "누적거래대금억": cumv(one), "거래량20봉배": vmul(one)} if one is not None else None),
                "눌림%": dip,
                "3번_1번고점재돌파": ({"시각": bs[three]["t"], "누적거래대금억": cumv(three), "거래량20봉배": vmul(three)} if three is not None else None),
            }
        out[f"{tf}분봉"] = {**extra, "빨간선_터치있음": bool(touch300), "빨간선_첫터치": touch300[0] if touch300 else None,
                           "빨간선_몸통터치있음": bool(body300), "빨간선_몸통첫터치": body300[0] if body300 else None,
            "빨간선_돌파": cross[:5], "빨간선_돌파있음": bool(cross),
            "빨간선_근접_시도": near[:5],
            "거래량+고점돌파": vol_brk[:5], "거래량돌파있음": bool(vol_brk),
            "봉수": len(idx),
        }
    return out


@app.get("/api/gom-minute-multi")
def gom_minute_multi(date: str = "", names: str = "", codes: str = ""):
    """🐻 분봉 연구 — 여러 종목 한 번에. 예: /api/gom-minute-multi?date=2026-11-03&names=매드업,펩트론"""
    d = (date or now_kst().strftime("%Y-%m-%d"))[:10]
    lst = [c.strip().upper() for c in codes.split(",") if c.strip()]
    unresolved = []
    for nm in [x.strip() for x in names.split(",") if x.strip()]:
        code = None
        try:
            r = supabase.table("stock_fundamentals").select("code").eq("name", nm).limit(1).execute().data
            code = r[0]["code"] if r else None
        except Exception:
            pass
        if not code:
            try:
                hits = _lookup_stock_code(nm)
                ex = [h for h in hits if h["name"].replace(" ", "") == nm.replace(" ", "")] or hits[:1]
                code = ex[0]["code"] if ex else None
            except Exception:
                code = None
        (lst.append(code) if code else unresolved.append(nm))
    nmap = {}
    try:
        for r in supabase.table("stock_fundamentals").select("code,name").in_("code", lst).execute().data or []:
            nmap[r["code"]] = r["name"]
    except Exception:
        pass
    rows = []
    for c in list(dict.fromkeys(lst))[:20]:
        try:
            f = _gom_minute_features(c, d)
            yn = lambda v: "O" if v else "X"
            rows.append({
                "종목": nmap.get(c, c),
                "3분_몸통터치": yn(f["3분봉"]["빨간선_몸통터치있음"]), "3분_몸통첫터치": f["3분봉"]["빨간선_몸통첫터치"],
                "3분_빨간선터치(꼬리포함)": yn(f["3분봉"]["빨간선_터치있음"]), "3분_첫터치": f["3분봉"]["빨간선_첫터치"],
                "3분_빨간선돌파": yn(f["3분봉"]["빨간선_돌파있음"]), "3분_돌파시각·자리": f["3분봉"]["빨간선_돌파"],
                "3분_근접시도": f["3분봉"]["빨간선_근접_시도"],
                "5분_빨간선돌파": yn(f["5분봉"]["빨간선_돌파있음"]), "5분_돌파시각·자리": f["5분봉"]["빨간선_돌파"],
                "5분_거래량+고점돌파": yn(f["5분봉"]["거래량돌파있음"]), "5분_거래량돌파시각": f["5분봉"]["거래량+고점돌파"],
                "3분_거래량+고점돌파": yn(f["3분봉"]["거래량돌파있음"]),
                "전일몸통상단": f["3분봉"].get("전일몸통상단"), "몸통상단_첫돌파": f["3분봉"].get("전일몸통상단_첫돌파"),
                "전일종가": f["3분봉"].get("전일종가"), "3분_5이평첫터치": f["3분봉"].get("5이평첫터치"),
                "터치후_연속상승봉": f["3분봉"].get("터치후_최대연속상승봉"),
                "1번(전일종가도달)": f["3분봉"].get("1번_전일종가첫도달"), "눌림%": f["3분봉"].get("눌림%"),
                "3번(1번고점재돌파)": f["3분봉"].get("3번_1번고점재돌파"),
                "분봉수(3분)": f["3분봉"]["봉수"],
            })
        except Exception as e:
            rows.append({"종목": nmap.get(c, c), "오류": str(e)})
    return {"ok": True, "date": d, "rows": rows, "못찾은종목": unresolved}


@app.get("/api/jm-signals")
def jm_signals(code: str):
    """🐻 곰발 · ▲ 역배정초 날짜 목록 (차트에 점 찍기용)"""
    try:
        return _jm_signal_series((code or "").strip().upper())
    except Exception as e:
        return {"ok": False, "error": str(e), "g": [], "y": []}


def _sig_fill(codes):
    try:
        for c in codes:
            try:
                _sig_cache[c] = (time.time(), _jm_signals_today(c))
            except Exception:
                _sig_cache[c] = (time.time(), {"g": None, "y": None})
            time.sleep(0.05)
    finally:
        _sig_busy["on"] = False


@app.get("/api/crab-pc")
def crab_pc_get():
    """PC 결과 + 현재가·등락률·거래대금·거래량(한투) · PC가 1분 넘게 소식 없으면 'PC 꺼짐'"""
    d, at = _crab_pc["data"], _crab_pc["at"]
    if not d:
        try:
            r = supabase.table("brief_cache").select("data,updated_at").eq("key", "crab_pc").limit(1).execute().data
            if r:
                d = r[0]["data"]
                at = datetime.fromisoformat(str(r[0]["updated_at"]).replace("Z", "+00:00")).timestamp()
                _crab_pc.update({"data": d, "at": at})
        except Exception:
            pass
    if not d:
        return {"ok": False, "alive": False, "error": "PC 프로그램에서 아직 결과가 안 왔어요 (세력트래커를 켜주세요)"}
    # 예전 이름(꽃게 시리즈)으로 보낸 결과나 오늘 것이 아닌 결과는 안 보여줌 (2026-11-02) — 새 PC 파일로 다시 켜면 바로 바뀜
    if not d.get("names") or str(d.get("at", ""))[:10] != now_kst().strftime("%Y-%m-%d"):
        return {"ok": False, "alive": time.time() - at < 150,
                "error": "오늘 PC 결과가 아직 없어요 — 새 crab_pc.py로 세력트래커를 다시 켜주세요"}
    alive = time.time() - at < 150
    v = _crab_pc_view
    if d.get("quotes_from_pc"):
        return _crab_pc_fast(d, alive)
    if v["data"] and time.time() - v["at"] < 30:
        out = dict(v["data"])
        out["alive"] = alive
        return out
    import concurrent.futures as cf
    KEYS = CRAB_KEYS
    codes = {x["code"] for k in KEYS for kk in (k, f"{k}_removed") for x in d.get(kk, [])}
    # 🎯 겹침 (2026-11-07): 종목마다 지금 어느 검색기에 같이 떠 있나
    member = {}
    for k in KEYS:
        for x in d.get(k, []) or []:
            member.setdefault(x["code"], set()).add(k)

    def q(code):
        try:
            return code, _kis_quote(code)
        except Exception:
            return code, {}

    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        quotes = dict(ex.map(q, list(codes)[:180]))
    # 🐻▲ 오늘 신호: 장중엔 10분, 장 마감 후엔 하루 보관 — 없거나 오래된 건 뒤에서 계산
    ttl = 600 if 9 <= now_kst().hour < 16 else 6 * 3600
    stale = [c for c in codes if not _sig_cache.get(c) or time.time() - _sig_cache[c][0] > ttl]
    for c0 in [c for c in stale if not _sig_cache.get(c)][:6]:  # 처음 보는 종목 몇 개는 바로 계산 (계속 '…'로 안 남게)
        try:
            _sig_cache[c0] = (time.time(), _jm_signals_today(c0))
        except Exception as e:
            print(f"[sig] {c0} 실패: {e}")
            _sig_cache[c0] = (time.time(), {"g": None, "y": None})
    stale = [c for c in stale if c not in _sig_cache or time.time() - _sig_cache[c][0] > ttl]
    if stale and not _sig_busy["on"]:
        _sig_busy["on"] = True
        threading.Thread(target=_sig_fill, args=(stale[:80],), daemon=True).start()

    def rows(lst, gone=False):
        out = []
        for x in lst:
            qq = quotes.get(x["code"]) or {}
            out.append({"code": x["code"], "name": x.get("name") or x["code"], "price": qq.get("price") or 0,
                        "pct": round(qq.get("pct") or 0, 2), "value_eok": round(qq.get("value_eok") or 0, 1), "volume": int(qq.get("volume") or 0),
                        "position": "—", "since": x.get("since"), "out": x.get("out") if gone else None,
                        "gone": gone, "sig": (_sig_cache.get(x["code"]) or (0, None))[1],
                        "in": sorted(member.get(x["code"], set())),
                        # 🎯 곰발+곰발 통합 · 🐻3 곰발+봉삼이 · ⬆ 완빨이 큰형님 자전거로 승격
                        "combo": [t for t, ks in (("🎯", {"big", "gomt"}), ("🐻3", {"big", "bong3"}), ("⬆", {"small", "crab"}))
                                  if ks <= member.get(x["code"], set())]})
        return sorted(out, key=lambda r: -r["value_eok"])

    out = {"ok": True, "source": "pc", "at": str(d.get("at", ""))[11:16], "alive": alive, "names": d.get("names") or {},
           "note": "🖥 HTS 조건검색 그대로 (PC 프로그램) · 서버는 KRX를 건드리지 않음"}
    for k in KEYS:
        out[k] = rows(d.get(k, []))
        out[f"{k}_removed"] = rows(d.get(f"{k}_removed", []), True)
    v.update({"at": time.time(), "data": out})
    return out


_crab_fill_px: dict = {}   # 체결이 아직 없어서 PC 시세가 비어 있는 종목만 한투로 한 번 채움 (code → (시각, quote))


def _crab_pc_fast(d: dict, alive: bool) -> dict:
    """⚡ 2026-11-07 JM: 사부님 화면처럼 빠르게 — PC(키움 실시간) 시세를 그대로 씀.
    종목명 · 현재가 · 등락률 · 거래대금만 (흑자/적자·일봉 신호 계산 등 무거운 건 뺌)"""
    member = {}
    for k in CRAB_KEYS:
        for x in d.get(k, []) or []:
            member.setdefault(x["code"], set()).add(k)
    need = [x["code"] for k in CRAB_KEYS for kk in (k, f"{k}_removed") for x in d.get(kk, []) or []
            if not x.get("pct") and not x.get("value_eok") and time.time() - _crab_fill_px.get(x["code"], (0, None))[0] > 300]

    def fill(codes):
        for c in codes:
            try:
                _crab_fill_px[c] = (time.time(), _kis_quote(c))
            except Exception:
                _crab_fill_px[c] = (time.time(), {})
            time.sleep(0.07)

    if need:
        for c in need[:30]:
            _crab_fill_px.setdefault(c, (time.time(), {}))  # 표시 먼저 (같은 걸 계속 맡기지 않게)
        threading.Thread(target=fill, args=(list(dict.fromkeys(need))[:30],), daemon=True).start()

    def rows(lst, gone=False):
        out = []
        for x in lst or []:
            px = x
            if not x.get("pct") and not x.get("value_eok"):
                px = {**x, **{k2: v2 for k2, v2 in (_crab_fill_px.get(x["code"], (0, {}))[1] or {}).items() if k2 in ("price", "pct", "value_eok", "volume")}}
            m = member.get(x["code"], set())
            out.append({"code": x["code"], "name": x.get("name") or x["code"], "price": px.get("price") or 0,
                        "pct": round(px.get("pct") or 0, 2), "value_eok": round(px.get("value_eok") or 0, 1),
                        "volume": int(px.get("volume") or 0), "since": x.get("since"), "out": x.get("out") if gone else None,
                        "gone": gone, "in": sorted(m),
                        "combo": [t for t, ks in (("🎯", {"big", "gomt"}), ("🐻3", {"big", "bong3"}), ("⬆", {"small", "crab"})) if ks <= m]})
        return sorted(out, key=lambda r: -r["value_eok"])

    out = {"ok": True, "source": "pc", "fast": True, "at": str(d.get("at", ""))[11:19], "alive": alive, "names": d.get("names") or {},
           "note": "🖥 HTS 조건검색 + 키움 실시간 시세 그대로"}
    for k in CRAB_KEYS:
        out[k] = rows(d.get(k, []))
        out[f"{k}_removed"] = rows(d.get(f"{k}_removed", []), True)
    return out


# 🌙 스윙 검색기 (2026-10-31 JM) — 정규장 마감 후 PC가 조건식 8개를 한 번씩 검색해서 올림
SWING_NAMES = ["양산w", "거래대금 기준봉", "황금맥", "기준v", "노랑등대", "이제가나", "역배정초", "꽃열매"]
_swing = {"data": None, "at": 0.0}
_swing_view: dict = {}


@app.post("/api/swing-pc")
def swing_pc_post(payload: dict):
    _swing.update({"data": payload, "at": time.time()})
    _swing_view.clear()
    try:
        supabase.table("brief_cache").upsert({"key": "swing_pc", "data": payload, "updated_at": now_kst().isoformat()}).execute()
    except Exception:
        pass
    return {"ok": True}


@app.post("/api/swing-run")
def swing_run():
    """사이트의 '🔄 다시 조회' — PC에 스윙 검색기 조회를 맡김"""
    with _bpc_lock:
        _bpc["tasks"].append({"kind": "swing"})
    return {"ok": True, "pc_alive": time.time() - _bpc["alive_at"] < 20}


def _run_swing_after_close():
    now = now_kst()
    if now.weekday() >= 5 or not ((15, 45) <= (now.hour, now.minute) <= (17, 0)):
        return
    day = now.strftime("%Y-%m-%d")
    if _brief_already_sent("job_swing", day) or time.time() - _bpc["alive_at"] > 20:
        return
    with _bpc_lock:
        _bpc["tasks"].append({"kind": "swing"})
    _mark_brief_sent("job_swing", day)


@app.get("/api/swing-screeners")
def swing_screeners(name: str = ""):
    d = _swing["data"]
    if not d:
        try:
            r = supabase.table("brief_cache").select("data").eq("key", "swing_pc").limit(1).execute().data
            if r:
                d = r[0]["data"]
                _swing.update({"data": d, "at": time.time()})
        except Exception:
            pass
    if not d:
        return {"ok": False, "error": "아직 스윙 검색 결과가 없어요 (정규장 마감 후 PC가 켜져 있으면 자동, 또는 🔄 다시 조회)"}
    sc = d.get("screeners", {})
    norm = lambda t: re.sub(r"\s", "", t or "")
    summary = []
    for nm in SWING_NAMES:
        hit = next((v for k, v in sc.items() if norm(k) == norm(nm)), None)
        summary.append({"name": nm, "count": len(hit["codes"]) if hit and not hit.get("missing") else 0,
                        "missing": (hit or {}).get("missing", hit is None)})
    out = {"ok": True, "at": d.get("at", ""), "screeners": summary}
    if name:
        hit = next((v for k, v in sc.items() if norm(k) == norm(name)), None)
        codes = (hit or {}).get("codes", [])[:150]
        key = (name, d.get("at"))
        if key not in _swing_view:
            import concurrent.futures as cf

            def q(x):
                try:
                    v = _kis_quote(x["code"])
                except Exception:
                    v = {}
                # 🔴 빨간선 자동 추천 (관심종목과 같은 기준) — 퇴근 후 칸 나누기용 (2026-11-01)
                try:
                    rl = _redline_classify(x["code"])
                except Exception:
                    rl = {}
                pos = (f"{rl['suggest']} · {rl['gap']:+.0f}%" if rl.get("suggest") else
                       (f"돌파 전 · {rl['gap']:+.0f}%" if rl.get("gap") is not None else "—"))
                return {"code": x["code"], "name": x.get("name") or x["code"], "price": v.get("price") or 0,
                        "pct": round(v.get("pct") or 0, 2), "value_eok": round(v.get("value_eok") or 0, 1), "volume": int(v.get("volume") or 0),
                        "position": pos, "why": rl.get("why"), "suggest": rl.get("suggest")}

            with cf.ThreadPoolExecutor(max_workers=4) as ex:
                _swing_view[key] = sorted(ex.map(q, codes[:80]), key=lambda r: -r["value_eok"])
        out["rows"] = _swing_view[key]
        out["name"] = name
    return out


# ══════════════════════════════════════════════════════════════════════════
# ⭐ 관심종목 리뉴얼 (2026-10-31 JM) — 당일매매 + 차트의 정석 + 파동 12칸 · 빨간선(볼린저 300, 2 상단) 자동 추천
# ══════════════════════════════════════════════════════════════════════════
DAYTRADE_CATS = ["당일 JM", "당일 눌림"]  # 🎯 당일매매 두 가지 (2026-11-01 JM)
JMW_CATS = ["당일 JM", "당일 눌림", "1번", "2번", "3번", "3-1번", "3-2번", "상승1파", "하락2파", "상승3파", "하락4파", "상승5파", "하락abc"]
_redline_cache: dict = {}  # code → (day, 결과)


def _redline_classify(code: str) -> dict:
    """🔴 빨간선(볼린저 300, 2 상단)으로 차트의 정석 위치 추천 — JM 기준
    돌파: 돌파 전 빨간선 하락 = 1번 / 빨간선 완만 = 3번
    이탈: 이탈 후 급락 = 2번 / 이탈 후 올랐다 내렸다 반복(매물소화·하방매집) = 3-1번"""
    today = now_kst().strftime("%Y-%m-%d")
    c = _redline_cache.get(code)
    if c and c[0] == today:
        return c[1]
    end = now_kst().strftime("%Y-%m-%d")
    start = (now_kst() - timedelta(days=560)).strftime("%Y-%m-%d")
    cs = _kis_daily_candles(code, start, end)  # 한투 일봉 (KRX 부담 없음)
    res = {"suggest": None, "why": "데이터 부족", "gap": None, "slope": None}
    if len(cs) >= 320:
        cl = [x["close"] for x in cs]
        up = []
        for i in range(len(cl)):
            if i < 299:
                up.append(None)
                continue
            w = cl[i - 299:i + 1]
            m = sum(w) / 300
            sd = (sum((v - m) ** 2 for v in w) / 300) ** 0.5
            up.append(m + 2 * sd)
        n = len(cl) - 1
        gap = (cl[n] / up[n] - 1) * 100
        slope = lambda a, b: (up[b] / up[a] - 1) * 100 if up[a] and up[b] else 0.0
        above = [up[i] is not None and cl[i] > up[i] for i in range(len(cl))]
        # 최근 돌파·이탈 시점 찾기 (최근 60봉)
        cross_up = next((i for i in range(n, max(300, n - 60), -1) if above[i] and not above[i - 1]), None)
        cross_dn = next((i for i in range(n, max(300, n - 60), -1) if not above[i] and above[i - 1]), None)
        res.update({"gap": round(gap, 1), "slope": round(slope(n - 20, n), 2)})
        if above[n]:
            i0 = cross_up if cross_up is not None else n
            before = slope(max(300, i0 - 30), i0)  # 돌파 전 30봉 동안 빨간선 기울기
            if before < -0.3:
                res.update({"suggest": "1번", "why": f"빨간선 돌파 · 돌파 전 빨간선 하락({before:+.1f}%)"})
            else:
                res.update({"suggest": "3번", "why": f"빨간선 돌파 · 빨간선 완만({before:+.1f}%)"})
        elif cross_dn is not None:
            seg = cl[cross_dn:n + 1]
            low_gap = min((cl[i] / up[i] - 1) * 100 for i in range(cross_dn, n + 1))
            swings = sum(1 for i in range(1, len(seg) - 1) if (seg[i] - seg[i - 1]) * (seg[i + 1] - seg[i]) < 0)
            if low_gap <= -12 and len(seg) <= 15:
                res.update({"suggest": "2번", "why": f"빨간선 이탈 후 급락(빨간선 대비 {low_gap:.0f}%)"})
            elif low_gap > -12 and swings >= 3:
                res.update({"suggest": "3-1번", "why": f"빨간선 이탈 후 오르내림 반복({swings}번) · 매물소화·하방매집"})
            else:
                res.update({"suggest": "2번", "why": f"빨간선 이탈 ({low_gap:.0f}%)"})
        else:
            res.update({"suggest": None, "why": f"빨간선 아래 ({gap:.0f}%) · 아직 돌파 전"})
    _redline_cache[code] = (today, res)
    return res


# ── ⭐ 관심종목 칸 정보: 흑자/적자 · 컨센서스 · 업종 · 테마 (2026-11-01) ──
_jmw_info_cache: dict = {}  # code → (day, info)
_theme_index = {"day": "", "map": {}}


def _build_theme_index():
    """종목 → 테마 이름 (네이버 테마 전체를 하루 한 번 훑어서 거꾸로 묶음, KRX 안 씀)"""
    day = now_kst().strftime("%Y-%m-%d")
    if _theme_index["day"] == day and _theme_index["map"]:
        return _theme_index["map"]
    try:
        r = supabase.table("brief_cache").select("data").eq("key", "theme_index").limit(1).execute().data
        if r and r[0]["data"].get("day") == day:
            _theme_index.update(r[0]["data"])
            return _theme_index["map"]
    except Exception:
        pass
    mp = {}
    try:
        for t in fetch_naver_theme_list(limit=300):
            try:
                stocks, _ = fetch_naver_theme_detail(t["theme_no"], limit=60)
            except Exception:
                continue
            for x in stocks:
                mp.setdefault(x["stock_code"], []).append(t["theme_name"])
            time.sleep(0.15)
    except Exception as e:
        print(f"[theme_index] 실패: {e}")
    if mp:
        _theme_index.update({"day": day, "map": mp})
        try:
            supabase.table("brief_cache").upsert({"key": "theme_index", "data": {"day": day, "map": mp},
                                                  "updated_at": now_kst().isoformat()}).execute()
        except Exception:
            pass
    return mp


def _run_theme_index_job():
    now = now_kst()
    if now.weekday() < 5 and (7, 20) <= (now.hour, now.minute) <= (8, 30) and _theme_index["day"] != now.strftime("%Y-%m-%d"):
        _build_theme_index()


def _naver_consensus(code: str) -> dict:
    """네이버 증권 종합 정보에서 목표주가·투자의견 (있으면 컨센서스 있음)"""
    j = requests.get(f"https://m.stock.naver.com/api/stock/{code}/integration",
                     headers={"User-Agent": "Mozilla/5.0", "Referer": "https://m.stock.naver.com/"}, timeout=8).json()

    def deep(o, test, d=0):
        if not isinstance(o, (dict, list)) or d > 6:
            return None
        items = o.items() if isinstance(o, dict) else enumerate(o)
        for k, v in items:
            if isinstance(k, str) and test(k) and v not in (None, "") and not isinstance(v, (dict, list)):
                return v
        for _, v in (o.items() if isinstance(o, dict) else enumerate(o)):
            f = deep(v, test, d + 1)
            if f is not None:
                return f
        return None

    tp = deep(j, lambda k: k.lower() in ("pricetargetmean", "targetprice", "goalprice", "targetpricemean"))
    op = deep(j, lambda k: k.lower() in ("recommmean", "opinion", "recommendmean", "investopinion"))
    try:
        tpv = float(str(tp).replace(",", "")) if tp is not None else None
    except ValueError:
        tpv = None
    return {"has": bool(tpv), "target": tpv, "opinion": op}


def _jmw_info(code: str) -> dict:
    day = now_kst().strftime("%Y-%m-%d")
    c = _jmw_info_cache.get(code)
    if c and c[0] == day:
        return c[1]
    info = {"profit": None, "consensus": None, "sector": None, "themes": []}
    try:
        info["profit"] = fetch_dart_quarterly(code).get("latest_status")
    except Exception:
        pass
    try:
        info["consensus"] = _naver_consensus(code)
    except Exception:
        info["consensus"] = {"has": False}
    try:
        r = supabase.table("sector_stocks").select("sector_name").eq("code", code).limit(1).execute().data
        info["sector"] = r[0]["sector_name"] if r else None
    except Exception:
        pass
    if not _theme_index["map"] and not _theme_index.get("building"):
        _theme_index["building"] = True  # 테마 목록이 아직 없으면 뒤에서 한 번 만들기 (몇 분 걸림, 다음 조회부터 보임)
        threading.Thread(target=lambda: (_build_theme_index(), _theme_index.update({"building": False})), daemon=True).start()
    info["themes"] = (_theme_index["map"] or {}).get(code, [])[:3]
    if info["themes"] or _theme_index["map"]:
        _jmw_info_cache[code] = (day, info)  # 테마가 아직 없으면 저장 안 함 → 다음 조회 때 다시
    return info


_cons_cache: dict = {}      # code → (day, 컨센서스)
_jmw_heavy: dict = {}       # (code, category, start_at) → {"rl", "info", "fl", "at"}
_jmw_heavy_busy: dict = {}


def _jmw_fill_heavy(items, with_redline, cat):
    try:
        for it in items:
            rl, info, fl = {}, {}, None
            simple = it["category"] in DAYTRADE_CATS  # 🎯 당일매매는 심플하게: 세력평단선만 (2026-11-02 JM)
            if with_redline and not simple:
                try:
                    rl = _redline_classify(it["code"])
                except Exception:
                    rl = {}
            if not simple:
                try:
                    info = _jmw_info(it["code"])
                except Exception:
                    info = {}
            else:
                # 🎯 당일매매도 컨센서스만은 보이게 (2026-11-02 JM) — 네이버 한 번, 하루 보관
                day = now_kst().strftime("%Y-%m-%d")
                cc = _cons_cache.get(it["code"])
                if not cc or cc[0] != day:
                    try:
                        cc = (day, _naver_consensus(it["code"]))
                    except Exception:
                        cc = (day, {"has": False})
                    _cons_cache[it["code"]] = cc
                info = {"consensus": cc[1]}
            if simple:
                try:
                    a = _force_avg_line(it["code"], it["category"], it.get("start_at"), it.get("end_at"))
                    if a:
                        fl = {"avg": round(a["avg"]), "gap": round((a["last"]["close"] / a["avg"] - 1) * 100, 2), "tf": a["tf"],
                              "start": a["start"], "nu": bool(it.get("start_at"))}
                except Exception:
                    fl = None
            _jmw_heavy[(it["code"], it["category"], it.get("start_at"))] = {"rl": rl, "info": info, "fl": fl, "at": time.time()}
    finally:
        _jmw_heavy_busy[cat] = False


@app.get("/api/jmw")
def jmw_list(cat: str = "", with_redline: bool = True):
    try:
        q = supabase.table("jm_watchlist").select("*").order("added_at", desc=True)
        if cat:
            q = q.eq("category", cat)
        items = q.limit(400).execute().data or []
    except Exception as e:
        return {"ok": False, "error": str(e)}
    counts = {c: 0 for c in JMW_CATS}
    for it in items:
        counts[it["category"]] = counts.get(it["category"], 0) + 1
    if cat:
        # ⚡ 화면은 바로 뜨게 (2026-11-02): 시세만 바로 받고, 무거운 것(빨간선·실적·컨센서스·세력평단선)은
        #    보관해 둔 값을 쓰고 없거나 오래됐으면 뒤에서 새로 계산 → 다음 새로고침(1분)부터 채워짐
        #    (예전엔 다 계산할 때까지 기다리다 '서버 연결 실패'가 떴음)
        import concurrent.futures as cf
        now_t = time.time()
        need = []
        for it in items:
            h = _jmw_heavy.get((it["code"], it["category"], it.get("start_at")))
            ttl = 55 if it["category"] in DAYTRADE_CATS else 3600
            if not h or now_t - h["at"] > ttl:
                need.append(it)
        if need and not _jmw_heavy_busy.get(cat):
            _jmw_heavy_busy[cat] = True
            threading.Thread(target=_jmw_fill_heavy, args=(need, with_redline, cat), daemon=True).start()

        def quote(it):
            try:
                v = _kis_quote(it["code"])
            except Exception:
                v = {}
            h = _jmw_heavy.get((it["code"], it["category"], it.get("start_at"))) or {}
            return {**it, "price": v.get("price") or 0, "pct": round(v.get("pct") or 0, 2),
                    "value_eok": round(v.get("value_eok") or 0, 1), "volume": int(v.get("volume") or 0),
                    "redline": h.get("rl") or {}, "info": h.get("info") or {}, "fline": h.get("fl"), "loading": not h}

        with cf.ThreadPoolExecutor(max_workers=6) as ex:
            items = list(ex.map(quote, items))
    return {"ok": True, "cats": JMW_CATS, "counts": counts, "items": items}


@app.post("/api/jmw/add")
def jmw_add(payload: dict):
    code, cat = (payload.get("code") or "").strip(), payload.get("category") or "당일 눌림"
    if not code or cat not in JMW_CATS:
        return {"ok": False, "error": "종목코드·칸 확인"}
    if cat in DAYTRADE_CATS:
        n = len(supabase.table("jm_watchlist").select("id").eq("category", cat).execute().data or [])
        if n >= 10:
            return {"ok": False, "error": f"{cat}은 최대 10종목이에요"}
    supabase.table("jm_watchlist").upsert({"code": code, "name": payload.get("name") or code, "category": cat,
                                           "memo": payload.get("memo")}, on_conflict="code,category").execute()
    return {"ok": True}


@app.post("/api/jmw/move")
def jmw_move(payload: dict):
    """칸 옮기기 (같은 종목이 새 칸에 이미 있으면 옛 칸만 지움)"""
    rid, cat = payload.get("id"), payload.get("category")
    if cat not in JMW_CATS:
        return {"ok": False, "error": "칸 이름 확인"}
    row = (supabase.table("jm_watchlist").select("*").eq("id", rid).limit(1).execute().data or [None])[0]
    if not row:
        return {"ok": False, "error": "없는 종목"}
    if cat in DAYTRADE_CATS:
        n = len(supabase.table("jm_watchlist").select("id").eq("category", cat).execute().data or [])
        if n >= 10:
            return {"ok": False, "error": f"{cat}은 최대 10종목이에요"}
    supabase.table("jm_watchlist").upsert({"code": row["code"], "name": row["name"], "category": cat, "memo": row.get("memo")},
                                           on_conflict="code,category").execute()
    supabase.table("jm_watchlist").delete().eq("id", rid).execute()
    return {"ok": True}


@app.post("/api/jmw/remove")
def jmw_remove(payload: dict):
    """종목코드+칸으로 빼기 (🦀 ★ 다시 누를 때)"""
    supabase.table("jm_watchlist").delete().eq("code", payload.get("code")).eq("category", payload.get("category") or "당일 눌림").execute()
    return {"ok": True}


@app.post("/api/jmw/delete")
def jmw_delete(payload: dict):
    supabase.table("jm_watchlist").delete().eq("id", payload.get("id")).execute()
    return {"ok": True}


@app.post("/api/jmw/zone")
def jmw_zone(payload: dict):
    """당일 JM: 세력평단 시작·종료 (예: '10-28 13:36', 비우면 오늘 09:00 / 고점까지)"""
    # 🆕 뉴기준봉: 시작 한 점만 (종료 없음) — 그 봉부터 새 세력평단선 (2026-11-01 JM)
    supabase.table("jm_watchlist").update({"start_at": (payload.get("start") or "").strip() or None,
                                           "end_at": None}).eq("id", payload.get("id")).execute()
    _touch_state.pop(str(payload.get("code") or ""), None)
    return {"ok": True}


@app.post("/api/jmw/memo")
def jmw_memo(payload: dict):
    supabase.table("jm_watchlist").update({"memo": payload.get("memo")}).eq("id", payload.get("id")).execute()
    return {"ok": True}


# ══════════════════════════════════════════════════════════════════════════
# 🎯 당일매매 세력평단 터치 알림 (2026-11-01 JM) — ⭐ 관심종목 '당일매매' 종목
#   3분봉 ⛰ JM 방식(09:00 ~ 오늘 고점까지 평단, 고점 뒤는 수평) 세력평단선에
#   가격이 위에서 내려와 닿으면 → 당일매매 전용 텔레그램 봇(TELEGRAM_BOT_TOKEN_DAYTRADE)
#   · 1분마다 확인 · 한 번 닿으면 다시 +2% 위로 올라갔다 내려와야 다음 알림 · 종목당 하루 최대 3번
# ══════════════════════════════════════════════════════════════════════════
_touch_state: dict = {}  # code → {"day", "armed", "count", "last"}


def _parse_dt(txt: str, today: str):
    """'10-28 13:36' · '2026-10-28 13:36' · '13:36' → ('YYYY-MM-DD', 'HH:MM')"""
    t = (txt or "").strip()
    m = re.search(r"(?:(\d{4})-)?(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})", t)
    if m:
        y = m.group(1) or today[:4]
        return f"{y}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", f"{int(m.group(4)):02d}:{m.group(5)}"
    m = re.search(r"(\d{1,2}):(\d{2})", t)
    if m:
        return today, f"{int(m.group(1)):02d}:{m.group(2)}"
    return None


def _force_avg_line(code: str, mode: str, start: str = None, end: str = None):
    """🎯 당일매매 세력평단
    · 당일 눌림 (기존 방식·사부님 방식): 3분봉 · 오늘 09:00(또는 시작)부터 지금까지 쭉 누적
    · 당일 JM (JM 방식): 5분봉 · 시작(비우면 오늘 09:00, 여러 날 가능) ~ 종료(비우면 고점)까지, 그 뒤 수평"""
    today = now_kst().strftime("%Y-%m-%d")
    tf = 3 if mode == "당일 눌림" else 5
    st = _parse_dt(start, today) or (today, "09:00")
    en = _parse_dt(end, today) if end else None
    days = [today]
    if mode == "당일 JM" and st[0] < today:
        d = datetime.strptime(st[0], "%Y-%m-%d")
        days = []
        while d.strftime("%Y-%m-%d") <= today and len(days) < 12:
            if d.weekday() < 5:
                days.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
    raw = []
    for dd in days:
        if dd == today:
            cs, *_ = fetch_minute_candles_paginated(code, "J", "09:00")
        else:
            cs = (_minute_day_cached(code, dd) or {}).get("candles") or []
        for c in cs:
            raw.append((dd, c))
    bars, order = {}, []
    for dd, c in raw:
        hh, mm = int(c["time"][:2]), int(c["time"][3:5])
        if (hh, mm) < (9, 0) or (hh, mm) > (15, 30):
            continue
        k = (dd, hh * 60 + (mm // tf) * tf)
        b = bars.get(k)
        if not b:
            bars[k] = {"key": f"{dd} {k[1] // 60:02d}:{k[1] % 60:02d}", "open": c["open"], "high": c["high"], "low": c["low"],
                       "close": c["close"], "volume": c.get("volume", 0)}
            order.append(k)
        else:
            b["high"] = max(b["high"], c["high"])
            b["low"] = min(b["low"], c["low"])
            b["close"] = c["close"]
            b["volume"] += c.get("volume", 0)
    seq = [bars[k] for k in sorted(order)]
    s_key = f"{st[0]} {st[1]}"
    fr = next((i for i, b in enumerate(seq) if b["key"] >= s_key), None)
    if fr is None or len(seq) - fr < 2:
        return None
    to = len(seq) - 1
    if mode == "당일 JM":
        if en:
            e_key = f"{en[0]} {en[1]}"
            to = max(fr, max((i for i, b in enumerate(seq) if b["key"] <= e_key), default=to))
        else:
            to = max(range(fr, len(seq)), key=lambda i: seq[i]["high"])  # ⛰ 고점까지
    bv = bq = 0.0
    for b in seq[fr:to + 1]:
        p, o, v = b["close"], b["open"] or b["close"], b["volume"] or 0
        if p > 0 and v > 0:
            ratio = min(abs(p - o) / o * 5, 0.45)
            net = int(v * ratio) if p >= o else -int(v * ratio)
            q = max(0, (v + net) // 2)
            if q > 0:
                bv += p * q
                bq += q
    if bq <= 0:
        return None
    peak = max(b["high"] for b in seq[fr:])
    return {"avg": bv / bq, "peak": peak, "last": seq[-1], "tf": tf, "start": s_key, "n": len(seq) - fr}


def _daytrade_touch_check():
    now = now_kst()
    if now.weekday() >= 5 or not ((9, 6) <= (now.hour, now.minute) <= (10, 30)):  # ⏰ 10:30까지만 (2026-11-07 JM)
        return
    try:
        # 🎯 터치 알림은 '당일 눌림'(3분봉 기존 방식)만 — 당일 JM은 알림 없이 화면에서만 (2026-11-01 JM)
        rows = supabase.table("jm_watchlist").select("code,name,category,start_at,end_at").eq("category", "당일 눌림").limit(10).execute().data or []
    except Exception:
        return
    tok, chat = bot("daytrade")
    day = now.strftime("%Y-%m-%d")
    for r in rows:
        code = r["code"]
        st = _touch_state.get(code)
        if not st or st["day"] != day:
            st = {"day": day, "armed": False, "count": 0, "last": 0.0}
            _touch_state[code] = st
        if st["count"] >= 1:  # 🎯 종목당 하루 한 번만 (2026-11-02 JM) — 뉴기준봉을 새로 찍으면 다시 한 번
            continue
        try:
            a = _force_avg_line(code, r["category"], r.get("start_at"), r.get("end_at"))
        except Exception:
            continue
        if not a:
            continue
        avg, last = a["avg"], a["last"]
        px = last["close"]
        if px >= avg * 1.02:
            st["armed"] = True  # 평단보다 충분히 위 → 다음에 내려와 닿으면 알림
            continue
        touched = st["armed"] and last["low"] <= avg * 1.003 and px >= avg * 0.985
        if touched and time.time() - st["last"] > 600:
            st.update({"armed": False, "count": st["count"] + 1, "last": time.time()})
            gap = (px / avg - 1) * 100
            from_peak = (px / a["peak"] - 1) * 100
            how = "5분봉 ⛰ JM 방식" if r["category"] == "당일 JM" else "3분봉 기존 방식 (눌림)"
            label = "🆕 뉴기준 터치" if r.get("start_at") else "🎯 세력평단 터치"  # 뉴기준봉을 찍어두면 그 선 기준 (2026-11-01)
            msg = (f"{label} [{r['category']}] ({now.strftime('%H:%M')})\n"
                   f"{r.get('name') or code} {px:,.0f}원 · 세력평단 {avg:,.0f} ({gap:+.1f}%)\n"
                   f"고점 {a['peak']:,.0f}에서 {from_peak:.1f}% · {how} · 시작 {a['start'][5:]}\n"
                   f"🔗 https://jm-tracker.vercel.app/stock-analysis?code={code}")
            send_telegram(msg, tok, chat)
        time.sleep(0.2)


def _daytrade_touch_loop():
    time.sleep(60)
    while True:
        try:
            _daytrade_touch_check()
        except Exception as e:
            print(f"[daytrade_touch] 실패: {e}")
        time.sleep(60)


@app.get("/api/daytrade-touch")
def daytrade_touch_preview():
    """지금 당일매매 종목들의 3분봉 세력평단·거리 미리 보기 (알림은 안 보냄)"""
    out = []
    try:
        rows = supabase.table("jm_watchlist").select("code,name,category,start_at,end_at").in_("category", DAYTRADE_CATS).limit(20).execute().data or []
    except Exception as e:
        return {"ok": False, "error": str(e)}
    for r in rows:
        try:
            a = _force_avg_line(r["code"], r["category"], r.get("start_at"), r.get("end_at"))
        except Exception as e:
            a = None
        if a:
            px = a["last"]["close"]
            out.append({"code": r["code"], "name": r.get("name"), "category": r["category"], "tf": a["tf"], "start": a["start"],
                        "price": px, "avg": round(a["avg"]), "gap": round((px / a["avg"] - 1) * 100, 2),
                        "peak": a["peak"], "state": _touch_state.get(r["code"])})
        else:
            out.append({"code": r["code"], "name": r.get("name"), "error": "오늘 분봉 없음"})
    return {"ok": True, "items": out}


# ══════════════════════════════════════════════════════════════════════════
# 📋 매매일지 게시판 스냅샷 (2026-11-01 JM) — 일지를 등록할 때 그날 그 종목 정보를 한 번에 모아 같이 저장
#   그날 시세(일봉) · 종목 투자자별 순매수(한투) · 창구(그날이 오늘이면 PC·키움 + 한투) · 빨간선 · 흑자/적자 · 컨센서스 · 업종·테마
# ══════════════════════════════════════════════════════════════════════════
@app.get("/api/journal-snapshot")
def journal_snapshot(code: str, date: str = ""):
    code = (code or "").strip().upper()
    today = now_kst().strftime("%Y-%m-%d")
    d = (date or today)[:10]
    snap = {"code": code, "date": d, "at": now_kst().strftime("%Y-%m-%d %H:%M")}
    # 그날 시세
    try:
        start = (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
        cs = _kis_daily_candles(code, start, d)
        if cs:
            day = cs[-1]
            prev = cs[-2]["close"] if len(cs) >= 2 else None
            snap["day"] = {**day, "pct": round((day["close"] / prev - 1) * 100, 2) if prev else None,
                           "value_eok": round(day.get("amount", 0) / 1e8, 1)}
    except Exception as e:
        snap["day_error"] = str(e)
    # 그 종목 투자자별 순매수 (한투 — 최근 30일 안이면)
    try:
        j = kis_get("/uapi/domestic-stock/v1/quotations/inquire-investor", "FHKST01010900",
                    {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code})
        ymd = d.replace("-", "")
        row = next((x for x in (j.get("output") or []) if x.get("stck_bsop_date") == ymd), None)
        if row:
            f = lambda k: float(str(row.get(k) or 0).replace(",", "") or 0)
            snap["investor"] = {"개인": round(f("prsn_ntby_tr_pbmn") / 100, 1), "외국인": round(f("frgn_ntby_tr_pbmn") / 100, 1),
                                "기관": round(f("orgn_ntby_tr_pbmn") / 100, 1), "unit": "억원"}
    except Exception as e:
        snap["investor_error"] = str(e)
    # 창구 (오늘이면 PC·키움 전체 + 한투 외국계 합계)
    if d == today:
        try:
            b = broker_pc_get(code)
            if b.get("ok"):
                snap["broker_pc"] = {"at": b.get("at"), "foreign_net": b.get("foreign_net"),
                                     "foreign": [{k: x.get(k) for k in ("name", "net", "avg_buy", "avg20", "hold20")} for x in b.get("foreign", [])[:6]],
                                     "domestic_buy": [{k: x.get(k) for k in ("name", "net", "avg_buy")} for x in b.get("domestic_buy", []) if x.get("net", 0) > 0][:5],
                                     "domestic_sell": [{k: x.get(k) for k in ("name", "net")} for x in b.get("domestic_sell", []) if x.get("net", 0) < 0][:5]}
        except Exception:
            pass
        try:
            bs = broker_split_endpoint(code)
            if bs.get("ok"):
                snap["broker_kis"] = {"glob": bs.get("glob"), "buy": bs.get("buy"), "sell": bs.get("sell")}
        except Exception:
            pass
    try:
        snap["redline"] = _redline_classify(code)
    except Exception:
        pass
    try:
        snap["info"] = _jmw_info(code)
    except Exception:
        pass
    return {"ok": True, "snapshot": snap}


# 🟢 해외 종목 → 네이버페이 증권 (2026-11-01 JM) — 네이버는 '티커.거래소'(예: NVDA.O 나스닥) 코드라
#   후보를 차례로 확인해서 실제로 있는 화면으로 보내줌 (한 번 찾으면 기억)
_naver_world_cache: dict = {}


@app.get("/api/naver-world")
def naver_world_redirect(symbol: str):
    from fastapi.responses import RedirectResponse
    sym = (symbol or "").strip().upper()
    code = _naver_world_cache.get(sym)
    if not code:
        base = sym.split(".")[0].replace("-", ".")
        cands = [sym] if "." in sym else []
        cands += [f"{base}.O", f"{base}.N", f"{base}.K", f"{base}.A", base]
        for c in cands:
            try:
                r = requests.get(f"https://api.stock.naver.com/stock/{c}/basic", headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
                if r.ok and (r.json() or {}).get("stockName"):
                    code = c
                    break
            except Exception:
                continue
        if code:
            _naver_world_cache[sym] = code
    if code:
        return RedirectResponse(f"https://stock.naver.com/worldstock/stock/{code}/finance/overview")
    # 못 찾으면 네이버 증권 검색으로
    return RedirectResponse(f"https://stock.naver.com/search?q={requests.utils.quote(sym)}")


# ══════════════════════════════════════════════════════════════════════════
# 📊 지분 구조 (2026-11-02 JM) — 남은 보유비중이 누구인지
#   외국인(한투, 매일) · 최대주주+특수관계인 / 자사주(DART 정기보고서) · 5% 이상 대량보유(DART)
#   기관 보유비중은 공식 데이터가 없어서 → 5% 대량보유 중 운용사·연금으로 보여주고, 나머지 = 개인·기타(유통 물량)
# ══════════════════════════════════════════════════════════════════════════
_own_cache: dict = {}


def _dart_json(api: str, params: dict):
    r = requests.get(f"https://opendart.fss.or.kr/api/{api}.json", params={"crtfc_key": DART_API_KEY, **params}, timeout=10).json()
    return r.get("list") or [] if r.get("status") == "000" else []


def _pct(v):
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except Exception:
        return None


@app.get("/api/ownership")
def ownership(code: str):
    code = (code or "").strip().upper()
    c = _own_cache.get(code)
    if c and time.time() - c[0] < 6 * 3600:
        return c[1]
    out = {"ok": True, "code": code, "foreign": None, "major": None, "major_names": [], "treasury": None, "big5": [], "report": None}
    listed = None
    try:  # 외국인 (한투 현재가: 상장주식수·외국인 소진율 — 한도 100%인 대부분 종목은 소진율 = 지분율)
        j = kis_get("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100", {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code})
        o = j.get("output") or {}
        listed = _pct(o.get("lstn_stcn"))
        out["foreign"] = _pct(o.get("hts_frgn_ehrt"))
    except Exception as e:
        out["foreign_error"] = str(e)
    corp = None
    try:
        corp = _dart_corp_code(code)
    except Exception:
        pass
    if corp and DART_API_KEY:
        y = now_kst().year
        for yr, rc, nm in ((y, "11014", "3분기"), (y, "11012", "반기"), (y, "11013", "1분기"), (y - 1, "11011", "사업"), (y - 1, "11014", "3분기")):
            rows = _dart_json("hyslrSttus", {"corp_code": corp, "bsns_year": str(yr), "reprt_code": rc})
            if not rows:
                continue
            common = [r for r in rows if "보통" in (r.get("stock_knd") or "보통")]
            tot = next((r for r in common if (r.get("nm") or "").strip() in ("계", "합계")), None)
            if tot:
                out["major"] = _pct(tot.get("trmend_posesn_stock_qota_rt"))
            else:
                out["major"] = round(sum(_pct(r.get("trmend_posesn_stock_qota_rt")) or 0 for r in common), 2)
            out["major_names"] = [r.get("nm") for r in common if (r.get("nm") or "").strip() not in ("계", "합계")][:4]
            out["report"] = f"{yr} {nm}보고서"
            try:  # 자사주 (같은 보고서)
                tr = _dart_json("tesstkAcqsDspsSttus", {"corp_code": corp, "bsns_year": str(yr), "reprt_code": rc})
                tt = next((r for r in tr if "총계" in (r.get("acqs_mth1") or "") or "총계" in (r.get("acqs_mth2") or "")), None) or (tr[-1] if tr else None)
                q = _pct((tt or {}).get("trmend_qy"))
                if q and listed:
                    out["treasury"] = round(q / listed * 100, 2)
            except Exception:
                pass
            break
        try:  # 5% 이상 대량보유 — 보고자별 가장 최근 것
            rows = _dart_json("majorstock", {"corp_code": corp})
            latest = {}
            for r in rows:
                nm = (r.get("repror") or "").strip()
                if nm and (nm not in latest or (r.get("rcept_dt") or "") > (latest[nm].get("rcept_dt") or "")):
                    latest[nm] = r
            majors = set(out["major_names"])
            big = [{"name": nm, "ratio": _pct(r.get("stkrt")), "date": r.get("rcept_dt")} for nm, r in latest.items()
                   if nm not in majors and (_pct(r.get("stkrt")) or 0) >= 5]
            out["big5"] = sorted(big, key=lambda x: -(x["ratio"] or 0))[:5]
        except Exception:
            pass
    known = sum(v for v in (out["major"], out["foreign"], out["treasury"]) if v) + sum(b["ratio"] or 0 for b in out["big5"])
    out["rest"] = round(max(0.0, 100 - known), 2) if known else None
    out["note"] = "외국인=한투(매일) · 최대주주·자사주=DART 정기보고서 · 5%↑=DART 대량보유(최근 보고) · 기관 보유비중은 공식 데이터가 없어 5%↑ 운용사·연금으로 대신"
    _own_cache[code] = (time.time(), out)
    return out


@app.post("/api/watchlist-add")
def watchlist_add_endpoint(payload: dict):
    """☆ 별표 → 관심종목 추가 (이미 있으면 그대로)"""
    try:
        code, name = payload.get("code"), payload.get("name")
        if not code:
            return {"ok": False, "error": "종목코드 없음"}
        ex = supabase.table("watchlist").select("id").eq("code", code).limit(1).execute().data
        if ex:
            return {"ok": True, "added": False}
        supabase.table("watchlist").insert({"code": code, "name": name or code, "horizon": "short"}).execute()
        return {"ok": True, "added": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# 🏦 창구 국내/외국계 나눠 보기 + 외국계 창구 합계 (2026-10-29) — 한투 회원사(FHKST01010600)는 상위 5개 + 외국계 합계를 줌
_broker_split_cache: dict = {}


@app.get("/api/broker-split")
def broker_split_endpoint(code: str):
    c = _broker_split_cache.get(code)
    if c and time.time() - c[0] < 60:
        return c[1]
    try:
        data = kis_get("/uapi/domestic-stock/v1/quotations/inquire-member", tr_id="FHKST01010600",
                       params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code})
        out = data.get("output")
        row = (out[0] if isinstance(out, list) else out) or {}
        f = lambda k: float(str(row.get(k) or 0).replace(",", "") or 0)
        try:
            price = _kis_quote(code).get("price") or 0
        except Exception:
            price = 0
        def side(prefix_name, prefix_qty, prefix_glob):
            items = []
            for i in range(1, 6):
                nm = row.get(f"{prefix_name}{i}", "")
                if not nm:
                    continue
                q = f(f"{prefix_qty}{i}")
                items.append({"rank": i, "name": nm, "qty": q, "foreign": row.get(f"{prefix_glob}{i}") == "Y",
                              "eok": round(q * price / 1e8, 1) if price else None})
            return items
        buy = side("shnu_mbcr_name", "total_shnu_qty", "shnu_mbcr_glob_yn_")
        sell = side("seln_mbcr_name", "total_seln_qty", "seln_mbcr_glob_yn_")
        gb, gs = f("glob_total_shnu_qty"), f("glob_total_seln_qty")
        gn = f("glob_ntby_qty") or (gb - gs)
        res = {"ok": True, "code": code, "price": price,
               "buy": {"domestic": [x for x in buy if not x["foreign"]], "foreign": [x for x in buy if x["foreign"]]},
               "sell": {"domestic": [x for x in sell if not x["foreign"]], "foreign": [x for x in sell if x["foreign"]]},
               "glob": {"buy": gb, "sell": gs, "net": gn, "net_eok": round(gn * price / 1e8, 1) if price else None},
               "at": now_kst().strftime("%H:%M")}
        _broker_split_cache[code] = (time.time(), res)
        return res
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════
# 🏦 창구(증권사)별 매매 — PC(키움 opt10038) 중계 (2026-10-30)
#   사이트가 요청 → 서버에 줄 세움 → PC(broker_pc.py)가 3초마다 가져가서 키움으로 조회 → 결과를 올림
# ══════════════════════════════════════════════════════════════════════════
_bpc = {"queue": [], "results": {}, "alive_at": 0.0, "tracked": {}, "samples": {}, "tasks": []}
_pc_task_results: dict = {}  # 🧪 시험·🌅 NXT 결과 (kind → 결과)
_vp20: dict = {}  # 💰 창구별 20일 평단: code → {"day", "rows", "asked"}
# 💰 창구 평균단가(추정) — 같은 종목 창구 데이터를 2분마다 다시 받아, 늘어난 수량 × 그때 가격으로 평균을 냄 (2026-10-30)
#   tracked: 사이트에서 본 종목(1시간 동안 장중 자동 추적) · samples: {code: {"day", "list": [(시각, 가격, {창구: (매수, 매도)})]}}
_bpc_lock = threading.Lock()


@app.post("/api/broker-pc/request")
def broker_pc_request(code: str):
    # 💰 창구별 20일 평단은 종목당 하루 한 번만 (키움 조회가 창구 수만큼 들어서)
    today = now_kst().strftime("%Y-%m-%d")
    v = _vp20.get(code)
    if not v or (v["day"] != today and time.time() - v.get("asked", 0) > 600):
        names = [x["name"] for x in (_bpc["results"].get(code) or {}).get("rows", [])[:6]]
        names += [x["name"] for x in (_bpc["results"].get(code) or {}).get("rows", [])[-4:]]
        with _bpc_lock:
            _bpc["tasks"].append({"kind": "vp20", "code": code, "names": names})
        _vp20[code] = {"day": (v or {}).get("day", ""), "rows": (v or {}).get("rows", []), "asked": time.time()}
    with _bpc_lock:
        _bpc["tracked"][code] = time.time()  # 💰 평균단가용: 본 종목은 1시간 동안 2분마다 자동으로 다시 받음
        r = _bpc["results"].get(code)
        if r and time.time() - r["got"] < 60:
            return {"ok": True, "queued": False}  # 1분 안에 받은 게 있으면 그거 씀
        if code not in _bpc["queue"]:
            _bpc["queue"].append(code)
    return {"ok": True, "queued": True, "pc_alive": time.time() - _bpc["alive_at"] < 20}


@app.get("/api/broker-pc/next")
def broker_pc_next():
    # PC가 3초마다 물어보러 올 때 2분 간격 추적도 같이 (작업 반복기는 5분 주기라 여기서 돌림)
    try:
        _bpc["alive_at"] = time.time()
        _run_broker_pc_track()
    except Exception:
        pass
    with _bpc_lock:
        _bpc["alive_at"] = time.time()  # PC가 물어보러 옴 = 살아있음
        if _bpc["tasks"]:
            return {"task": _bpc["tasks"].pop(0)}  # 🧪 시험·🌅 NXT 먼저
        if _bpc["queue"]:
            return {"code": _bpc["queue"].pop(0)}
    return {}


@app.post("/api/broker-pc/result")
def broker_pc_result(payload: dict):
    code = payload.get("code")
    if code:
        with _bpc_lock:
            _bpc["results"][code] = {**payload, "got": time.time()}
            if len(_bpc["results"]) > 300:
                _bpc["results"].clear()
        # 💰 표본 저장 (장중만): 그때 가격 + 창구별 누적 매수·매도
        nowk = now_kst()
        if payload.get("rows") and nowk.weekday() < 5 and (9, 0) <= (nowk.hour, nowk.minute) <= (15, 35):
            try:
                px = _kis_quote(code).get("price") or 0
            except Exception:
                px = 0
            if px:
                day = nowk.strftime("%Y-%m-%d")
                sm = _bpc["samples"].get(code)
                if not sm or sm["day"] != day:
                    sm = {"day": day, "list": []}
                    _bpc["samples"][code] = sm
                sm["list"].append((nowk.strftime("%H:%M"), px, {x["name"]: (x["buy"], x["sell"]) for x in payload["rows"]}))
                sm["list"] = sm["list"][-300:]
        # 📅 장 마감 뒤(15:30~) 받은 건 그날 기록으로 저장 → 며칠째 사는지(연속성) 계산용
        now = now_kst()
        if payload.get("rows") and now.weekday() < 5 and (now.hour, now.minute) >= (15, 30):
            try:
                supabase.table("broker_pc_daily").upsert({"code": code, "day": now.strftime("%Y-%m-%d"),
                                                          "rows": [{"name": x["name"], "net": x["net"]} for x in payload["rows"]]}).execute()
            except Exception as e:
                print(f"[broker_pc] 기록 저장 실패: {e}")
    return {"ok": True}


def _run_broker_pc_track():
    """장중 2분마다: 사이트에서 최근 1시간 안에 본 종목을 PC에 다시 맡김 → 평균단가 표본이 쌓임"""
    now = now_kst()
    if now.weekday() >= 5 or not ((9, 0) <= (now.hour, now.minute) <= (15, 30)):
        return
    if time.time() - _bpc.get("track_at", 0) < 115 or time.time() - _bpc["alive_at"] > 20:
        return
    _bpc["track_at"] = time.time()
    # ⭐ 관심종목 '당일매매' 칸은 열어보지 않아도 장 내내 자동 추적 (2026-10-31)
    try:
        for r in supabase.table("jm_watchlist").select("code").in_("category", DAYTRADE_CATS).limit(20).execute().data or []:
            _bpc["tracked"][r["code"]] = time.time()
    except Exception:
        pass
    with _bpc_lock:
        live = {c: t for c, t in _bpc["tracked"].items() if time.time() - t < 3600}
        _bpc["tracked"] = live
        for c in list(live)[:10]:  # 키움 부담 줄이게 최대 10종목
            if c not in _bpc["queue"]:
                _bpc["queue"].append(c)


def _run_broker_pc_daily():
    """장 마감 뒤(15:35~15:50) 관심종목을 PC에 한꺼번에 맡겨서 그날 창구 기록을 자동으로 쌓음 (연속성 계산용)"""
    now = now_kst()
    if now.weekday() >= 5 or not ((15, 35) <= (now.hour, now.minute) <= (15, 50)):
        return
    day = now.strftime("%Y-%m-%d")
    if _brief_already_sent("job_broker_pc_daily", day):
        return
    if time.time() - _bpc["alive_at"] > 20:
        return  # PC가 꺼져 있으면 다음 확인 때 다시
    try:
        codes = [r["code"] for r in (supabase.table("watchlist").select("code").limit(40).execute().data or []) if r.get("code")]
        with _bpc_lock:
            for c in codes:
                if c not in _bpc["queue"]:
                    _bpc["queue"].append(c)
        _mark_brief_sent("job_broker_pc_daily", day)
        print(f"[broker_pc] 관심종목 {len(codes)}개 창구 기록 요청")
    except Exception as e:
        print(f"[broker_pc] 자동 기록 실패: {e}")


# 🟢 키움 통합 일봉 (PC 켜져 있을 때) — 사부님 곰발 ● 은 통합(KRX+NXT) 캔들 기준 (11/5 JM 발견)
_kw_daily: dict = {}   # 통합: code → (받은 시각, [{date, open, high, low, close, volume, amount}])
_kk_daily: dict = {}   # KRX 정규장만 (키움, 종목코드 그대로)
_nx_daily: dict = {}   # NXT만 (키움, 종목코드_NX) — 2026-11-12 기준가 비교용
_SRC_SUFFIX = {"krx": "", "al": "_AL", "nx": "_NX"}
_SRC_LABEL = {"krx": "키움 KRX 정규장", "al": "키움 통합(KRX+NXT)", "nx": "키움 NXT"}


def _daily_store(suffix: str) -> dict:
    return _kk_daily if suffix == "" else (_nx_daily if suffix == "_NX" else _kw_daily)


def _kiwoom_daily_al(codes: list, wait: int = 90, krx_only: bool = False, suffix: str = None) -> dict:
    """PC(키움)에 일봉을 맡기고 기다림. suffix: "" KRX 정규장 · "_AL" 통합 · "_NX" NXT (없으면 krx_only로 결정)
    PC가 꺼져 있으면 빈 값 → 한투로 대체"""
    if suffix is None:
        suffix = "" if krx_only else "_AL"
    store = _daily_store(suffix)
    need = [c for c in codes if not store.get(c) or time.time() - store[c][0] > 600]
    if need:
        if time.time() - _bpc["alive_at"] > 30:
            return {}
        asked = time.time()
        with _bpc_lock:
            _bpc["tasks"].append({"kind": "daily_al", "codes": need, "suffix": suffix})
        while time.time() - asked < wait:
            r = _pc_task_results.get("daily_al")
            if r and r.get("got", 0) > asked and r.get("suffix", "_AL") == suffix:
                break
            time.sleep(1)
    return {c: store[c][1] for c in codes if store.get(c)}


@app.post("/api/pc-task/result")
def pc_task_result(payload: dict):
    kind = payload.get("kind") or "unknown"
    _pc_task_results[kind] = {**payload, "got": time.time()}
    if kind == "daily_al":  # 🟢 키움 일봉 보관 — 통합(_AL) / KRX 정규장("") (2026-11-05)
        store = _daily_store(payload.get("suffix", "_AL"))
        for code, rows in (payload.get("data") or {}).items():
            if rows:
                store[code] = (time.time(), sorted(rows, key=lambda x: x["date"]))
    if kind == "vp20" and payload.get("code"):
        _vp20[payload["code"]] = {"day": now_kst().strftime("%Y-%m-%d"), "rows": payload.get("rows", []), "asked": time.time()}
    return {"ok": True}


@app.get("/api/pc-probe")
def pc_probe(code: str = "005930"):
    """🧪 키움 '거래원 매물대 분석'(opt10043) 시험 — PC가 받아서 해보고 결과를 /api/pc-task-result?kind=probe_vp 에 올림"""
    with _bpc_lock:
        _bpc["tasks"].append({"kind": "probe_vp", "code": code})
    return {"ok": True, "queued": True, "pc_alive": time.time() - _bpc["alive_at"] < 20,
            "next": "10~20초 뒤 /api/pc-task-result?kind=probe_vp 열어보기"}


@app.get("/api/pc-task-result")
def pc_task_result_get(kind: str):
    r = _pc_task_results.get(kind)
    return r or {"ok": False, "error": "아직 결과 없음 (PC가 켜져 있는지 확인)"}


# 🌅 NXT 프리마켓 (08:00~08:50) — PC가 키움으로 '종목코드_NX' 시세를 받아옴 (2026-10-30)
# 🌅 NXT 프리마켓에 항상 넣는 대형주 30 (시장이 어느 쪽으로 열릴지 읽기용, 2026-11-02)
NXT_BIG_CAPS = ["005930", "000660", "373220", "207940", "005380", "000270", "068270", "105560", "055550", "035420",
                "012450", "329180", "028260", "006400", "051910", "042660", "012330", "035720", "086790", "009540",
                "034020", "010140", "267260", "402340", "009150", "196170", "247540", "086520", "028300", "042700"]


def _nxt_candidates() -> list:
    codes = list(NXT_BIG_CAPS)
    try:
        codes += [r["code"] for r in (supabase.table("watchlist").select("code").limit(60).execute().data or []) if r.get("code")]
    except Exception:
        pass
    d = _crab_pc.get("data") or {}
    for k in CRAB_KEYS:
        codes += [x["code"] for x in d.get(k, []) or []]
    try:
        r = supabase.table("brief_cache").select("data").eq("key", "new_highs").limit(1).execute().data
        if r:
            codes += [x.get("code") for x in (r[0]["data"].get("rows") or r[0]["data"].get("items") or []) if x.get("code")]
    except Exception:
        pass
    seen, out = set(), []
    for c in codes:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out[:120]


def _run_nxt_premarket():
    now = now_kst()
    if now.weekday() >= 5:
        return
    hm = (now.hour, now.minute)
    day = now.strftime("%Y-%m-%d")
    if (8, 15) <= hm < (8, 40) and not _brief_already_sent("job_nxt_gom_pre", day):
        _mark_brief_sent("job_nxt_gom_pre", day)
        threading.Thread(target=_nxt_gom_prefetch, daemon=True).start()
    if (8, 5) <= hm < (8, 45) and time.time() - _bpc["alive_at"] < 20:
        slot = "a" if hm < (8, 25) else "b"
        if not _brief_already_sent(f"job_nxt_{slot}", day):
            with _bpc_lock:
                _bpc["tasks"].append({"kind": "nxt", "codes": _nxt_candidates()})
            _mark_brief_sent(f"job_nxt_{slot}", day)
    if (8, 40) <= hm <= (8, 59) and not _brief_already_sent("nxt_premarket", day):
        r = _pc_task_results.get("nxt")
        fresh = r and time.time() - r.get("got", 0) <= 1200
        if not fresh:
            # 🔁 PC가 중간에 꺼졌다 켜져서 08:25 조회를 놓쳤으면 → 지금 다시 맡기고 다음 확인(몇 분 뒤)에 보냄 (2026-11-03)
            if time.time() - _bpc["alive_at"] < 20 and time.time() - _bpc.get("nxt_retry_at", 0) > 240:
                _bpc["nxt_retry_at"] = time.time()
                with _bpc_lock:
                    _bpc["tasks"].append({"kind": "nxt", "codes": _nxt_candidates()})
                print("[nxt] 최근 결과가 없어서 다시 조회 맡김")
                return
            _alert_clock_state["nxt_status"] = f"{now_kst().strftime('%H:%M')} PC NXT 결과 기다리는 중"
            if hm >= (8, 55):
                # 끝까지 못 받았으면 조용히 넘어가지 말고 한 번 알려줌
                _mark_brief_sent("nxt_premarket", day)
                send_telegram("🌅 NXT 프리마켓 — 오늘은 PC(세력트래커)에서 NXT 시세를 못 받았어요 (중간에 꺼졌거나 오류). 세력트래커 상태만 확인해 주세요.")
            return
        rows = [x for x in r.get("rows", []) if x.get("price") and x.get("pct") is not None and x.get("volume")]
        if not rows:  # 🔧 2026-11-09: 예전엔 여기서 조용히 끝났음 → 이젠 안내 + 🐾 어제 곰발은 그대로 보냄
            print("[nxt] 시세가 비어 있음 — 키움이 _NX 시세를 안 주는 듯")
            _alert_clock_state["nxt_status"] = f"{now_kst().strftime('%H:%M')} NXT 시세 비어 있음 (PC 결과 {len(r.get('rows', []))}행)"
        big = [x for x in rows if x["code"] in NXT_BIG_CAPS]
        mood = ""
        if big:
            avg = sum(x["pct"] for x in big) / len(big)
            ups = sum(1 for x in big if x["pct"] > 0)
            mood = f"대형주 {len(big)}개 평균 {avg:+.2f}% · 상승 {ups} / 하락 {len(big) - ups} → " + \
                   ("🔴 강하게 열릴 듯" if avg >= 0.7 else "🔵 약하게 열릴 듯" if avg <= -0.7 else "⚪ 보합 출발 예상")
        up = sorted(rows, key=lambda x: -x["pct"])[:8]
        dn = sorted(rows, key=lambda x: x["pct"])[:3]
        link = lambda c: f"https://jm-tracker.vercel.app/stock-analysis?code={c}"
        lines = ([f"🌅 NXT 프리마켓 ({r.get('at', '')[:5]} 기준 · {len(rows)}종목)"] + ([f"📊 {mood}"] if mood else []) + ["▲ 강한 종목"]) if rows else \
                ["🌅 NXT 프리마켓 — 오늘은 NXT 시세가 비어서 왔어요 (키움이 프리마켓 시세를 0으로 줌). 🐾 어제 곰발만 보내요"]
        lines += [f"· {x.get('name') or x['code']} {x['pct']:+.2f}% · {x['price']:,}원 · {x['volume']:,}주\n  🔗 {link(x['code'])}" for x in up if x["pct"] > 0]
        weak = [x for x in dn if x["pct"] < 0]
        if weak:
            lines.append("▼ 약한 종목")
            lines += [f"· {x.get('name') or x['code']} {x['pct']:+.2f}%" for x in weak]
        if rows:
            lines.append("※ 대형주 30 + 관심종목·검색기·52주 신고가 후보 (NXT 프리마켓은 거래가 적어 방향 참고용 — 매매는 KRX 정규장)")
        # 🐻 어제 곰발 포착 종목 (2026-11-07 JM) — NXT와 관계없이 어제 통합 종가 기준 · 거래대금 순
        gday, gitems = _gom_prev_day_items()
        info = dict(_nxt_gom_info["rows"]) if _nxt_gom_info.get("day") == day else {}
        snap = _gom_snap_load(gday) if gday else {}
        for c, v in snap.items():  # 🐾 어제 저녁에 저장한 값이 먼저 (2026-11-10)
            info[c] = {**info.get(c, {}), **{k: x for k, x in v.items() if x not in (None, "")}}
        if gitems:
            lines.append(f"\n🐾 어제({gday[5:].replace('-', '/')}) 곰발 포착 {len(gitems)}종목 (어제 정규장 종가 · 거래대금+거래량 순위)")
            # 🏆 2026-11-10 JM: 거래대금 순위 + 거래량 순위 → 두 순위 합이 작은 순
            ra = {c: i for i, c in enumerate(sorted(gitems, key=lambda c: -(info.get(c, {}).get("amount") or 0)), 1)}
            rv = {c: i for i, c in enumerate(sorted(gitems, key=lambda c: -(info.get(c, {}).get("volume") or 0)), 1)}
            ordered = sorted(gitems, key=lambda c: (ra[c] + rv[c], ra[c]))
            for i, c in enumerate(ordered, 1):
                nm = gitems[c]
                inf = info.get(c, {})
                fr = inf.get("foreign")
                ftxt = (f"외국인 🔴매수 {fr:+,.0f}억" if fr > 0 else f"외국인 🔵매도 {fr:,.0f}억" if fr < 0 else "외국인 ⚪0") if fr is not None else "외국인 -"
                st = inf.get("status") or ""
                op = inf.get("op")
                ptxt = ("🟠적자" if ("적자" in st and "흑자전환" not in st) or (not st and op is not None and op < 0)
                        else "🟢흑자" if st or (op is not None and op >= 0) else "실적 -")
                if "전환" in st:
                    ptxt += f"({st})"
                if inf.get("price"):
                    lines.append(f"{i}. {nm} {inf['price']:,.0f}원 ({inf['pct']:+.2f}%) · {inf['amount'] / 1e8:,.0f}억 · {inf['volume']:,.0f}주 (대금 {ra[c]}위·거래량 {rv[c]}위)"
                                 f"\n   {ftxt} · {ptxt}\n   🔗 {link(c)}")
                else:
                    lines.append(f"{i}. {nm} · {ftxt} · {ptxt}\n   🔗 {link(c)}")
            lines.append("※ 가격·등락률·거래대금 = 어제 정규장 마감(15:45 저장) · 외국인 = 어제 순매수(18:10 저장) · 흑자/적자 = 최근 분기 영업이익")
        else:
            lines.append("\n🐾 어제 곰발 포착 기록이 없어요 — 세력트래커가 어제 장중에 곰발 목록을 못 올렸거나, 저장을 시작한 첫날이에요")
        if send_telegram("\n".join(lines)):
            _mark_brief_sent("nxt_premarket", day)
            if rows:
                _alert_clock_state["nxt_status"] = f"{now_kst().strftime('%H:%M')} 보냄 · NXT {len(rows)}종목 · 🐾 곰발 {len(gitems)}종목"
            else:
                _alert_clock_state["nxt_status"] += f" → 안내 + 🐾 곰발 {len(gitems)}종목 보냄"
        else:
            _alert_clock_state["nxt_status"] = f"{now_kst().strftime('%H:%M')} 텔레그램 전송 실패 — 다음 확인 때 다시"


@app.get("/api/nxt-premarket")
def nxt_premarket_endpoint(run: bool = False):
    if run:
        with _bpc_lock:
            _bpc["tasks"].append({"kind": "nxt", "codes": _nxt_candidates()})
        return {"ok": True, "queued": True, "next": "30초~1분 뒤 run 없이 다시 열기"}
    return _pc_task_results.get("nxt") or {"ok": False, "error": "아직 결과 없음"}


@app.get("/api/broker-pc")
def broker_pc_get(code: str):
    alive = time.time() - _bpc["alive_at"] < 20
    r = _bpc["results"].get(code)
    if not r:
        return {"ok": False, "pc_alive": alive, "waiting": code in _bpc["queue"] or alive}
    try:
        price = _kis_quote(code).get("price") or 0
    except Exception:
        price = 0
    p5 = {x["name"]: x["net"] for x in r.get("p5", []) or []}
    p20 = {x["name"]: x["net"] for x in r.get("p20", []) or []}
    # 📅 연속성: 저장된 지난 날들 + 오늘 순매수 방향이 같은 날이 며칠 이어졌나
    hist = []
    try:
        hist = supabase.table("broker_pc_daily").select("day,rows").eq("code", code).order("day", desc=True).limit(15).execute().data or []
    except Exception:
        pass
    today = now_kst().strftime("%Y-%m-%d")
    past = [{x["name"]: x["net"] for x in h["rows"]} for h in hist if str(h["day"]) != today]

    def streak(name, net):
        if not net:
            return 0
        n = 1
        for day in past:
            v = day.get(name, 0)
            if v and (v > 0) == (net > 0):
                n += 1
            else:
                break
        return n

    # 💰 평균단가(추정): 표본 사이에 늘어난 매수(매도) 수량 × 그 구간 가격 — 첫 표본 이전 물량은 첫 표본 가격으로 봄
    sm = _bpc["samples"].get(code)
    avg = {}
    if sm and sm["day"] == now_kst().strftime("%Y-%m-%d") and sm["list"]:
        prev, prev_px = {}, None
        for t, px, mp in sm["list"]:
            use_px = px if prev_px is None else (px + prev_px) / 2
            for nm, (b, sl) in mp.items():
                pb, ps = prev.get(nm, (0, 0))
                a = avg.setdefault(nm, [0.0, 0, 0.0, 0])
                db, ds = max(0, b - pb), max(0, sl - ps)
                a[0] += db * use_px
                a[1] += db
                a[2] += ds * use_px
                a[3] += ds
            prev, prev_px = mp, px
    samples_n = len(sm["list"]) if sm else 0
    since = sm["list"][0][0] if sm and sm["list"] else None

    def avgs(nm):
        a = avg.get(nm)
        if not a:
            return None, None
        return (round(a[0] / a[1]) if a[1] else None), (round(a[2] / a[3]) if a[3] else None)

    norm = lambda t: re.sub(r"\s", "", t or "")
    v20 = {norm(x["name"]): x for x in (_vp20.get(code) or {}).get("rows", [])}
    rows = []
    for x in r.get("rows", []):
        ab, asl = avgs(x["name"])
        w = v20.get(norm(x["name"])) or {}
        rows.append({**x, "net_eok": round(x["net"] * price / 1e8, 1) if price else None,
                     "p5": p5.get(x["name"]), "p20": p20.get(x["name"]), "streak": streak(x["name"], x["net"]),
                     "avg_buy": ab, "avg_sell": asl,
                     "avg20": w.get("avg_hold"), "hold20": w.get("hold"), "net20": w.get("net20")})
    fr = [x for x in rows if x.get("foreign")]
    dom = [x for x in rows if not x.get("foreign")]
    return {"ok": True, "pc_alive": alive, "at": r.get("at"), "error": r.get("error"), "price": price,
            "avg_samples": samples_n, "avg_since": since,
            "foreign": sorted(fr, key=lambda x: -abs(x["net"])),
            "domestic_buy": sorted(dom, key=lambda x: -x["net"])[:5],
            "domestic_sell": sorted(dom, key=lambda x: x["net"])[:5],
            "foreign_net": sum(x["net"] for x in fr)}


# ══════════════════════════════════════════════════════════════════════════
# 🚨 PC 꺼짐 알림 (하루 1회) — 장중 세력트래커가 5분 넘게 소식 없으면 JM 전용으로 (2026-10-30)
#   PC는 창구 연동(3초마다)·꽃게(1분마다)로 계속 신호를 보냄 → 둘 다 끊기면 꺼진 것
# ══════════════════════════════════════════════════════════════════════════
_SERVER_STARTED = time.time()


def _run_pc_off_alert():
    now = now_kst()
    if now.weekday() >= 5 or not ((9, 5) <= (now.hour, now.minute) <= (15, 20)):
        return
    if time.time() - _SERVER_STARTED < 600:
        return  # 서버가 막 켜졌을 땐 PC 신호를 아직 못 받았을 수 있어서 10분 기다림
    last = max(_bpc.get("alive_at", 0.0), _crab_pc.get("at", 0.0))
    if time.time() - last < 300:
        return
    day = now.strftime("%Y-%m-%d")
    if _brief_already_sent("pc_off_alert", day):
        return
    ago = "오늘 아직 신호 없음" if not last else f"{int((time.time() - last) / 60)}분째 신호 없음"
    send_telegram(f"🚨 PC(세력트래커) 꺼진 것 같아요 ({ago})\n"
                  f"🦀 꽃게 검색기 · 🏦 키움 창구별 · 💰 창구 평단이 멈춰요\n"
                  f"→ 원격으로 PC 확인 후 세력트래커.bat 다시 실행\n(오늘은 이 알림 한 번만 보내요)")
    _mark_brief_sent("pc_off_alert", day)


# ══════════════════════════════════════════════════════════════════════════
# 📰 주간 정리 (금요일 22:00, JM 리포트) — 월~금 국내증시·해외증시·국내뉴스·해외뉴스 (2026-10-30)
#   숫자는 시세에서 직접, 뉴스는 이번 주 제목 중 영향 큰 것 (AI 문장 생성 아님, 비용 0)
# ══════════════════════════════════════════════════════════════════════════
def _week_change(sym: str, monday: datetime):
    """이번 주 등락: (지난주 마지막 종가 → 지금)"""
    j = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}",
                     params={"range": "1mo", "interval": "1d"}, headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
    res = j["chart"]["result"][0]
    ts = res.get("timestamp") or []
    cl = res["indicators"]["quote"][0].get("close") or []
    rows = [(datetime.fromtimestamp(t, KST).date(), c) for t, c in zip(ts, cl) if c is not None]
    before = [c for d, c in rows if d < monday.date()]
    last = (res.get("meta") or {}).get("regularMarketPrice") or (rows[-1][1] if rows else None)
    if not before or not last:
        return None, None
    return last, (last / before[-1] - 1) * 100


def build_weekly_review():
    now = now_kst()
    monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    friday = monday + timedelta(days=4)
    title = f"🟥🦀【주간 정리】 {monday.month}/{monday.day}(월) ~ {friday.month}/{friday.day}(금)"
    parts = [title, "━━━━━━━━━━━━"]

    def line(label, sym, unit=""):
        try:
            px, ch = _week_change(sym, monday)
            if px is None:
                return None
            arrow = "🔴▲" if ch > 0 else "🔵▼" if ch < 0 else "⚪"
            return f"{label} {px:,.2f}{unit} {arrow}{abs(ch):.2f}%"
        except Exception:
            return None

    # 🇰🇷 국내증시
    kr = [x for x in (line("코스피", "^KS11"), line("코스닥", "^KQ11"), line("원/달러", "KRW=X", "원")) if x]
    try:
        for mk, nm in (("KOSPI", "코스피"), ("KOSDAQ", "코스닥")):
            df = _krx_df(stock.get_market_trading_value_by_date, monday.strftime("%Y%m%d"), now.strftime("%Y%m%d"), mk)
            if df is not None and not df.empty:
                sm = df.sum()
                f = lambda k: sm.get(k, 0) / 1e8
                kr.append(f"{nm} 수급 · 개인 {f('개인'):+,.0f}억 · 외국인 {f('외국인합계') or f('외국인'):+,.0f}억 · 기관 {f('기관합계'):+,.0f}억")
    except Exception as e:
        print(f"[weekly] 수급 실패: {e}")
    parts.append("🇰🇷 국내증시\n" + ("\n".join(f"· {x}" for x in kr) if kr else "· (데이터 없음)"))

    # 🌎 해외증시 (미국은 금요일 밤 장 시작 전이라 목요일 종가까지)
    gl = [x for x in (line("S&P500", "^GSPC"), line("나스닥", "^IXIC"), line("다우", "^DJI"), line("필라델피아반도체", "^SOX"),
                      line("니케이", "^N225"), line("대만 가권", "^TWII"), line("홍콩 항셍", "^HSI"),
                      line("MSCI 한국(EWY)", "EWY"), line("MSCI 신흥국(EEM)", "EEM"),
                      line("달러인덱스", "DX-Y.NYB"), line("美 10년물", "^TNX"), line("WTI", "CL=F")) if x]
    parts.append("🌎 해외증시 (미국은 목요일 종가까지)\n" + ("\n".join(f"· {x}" for x in gl) if gl else "· (데이터 없음)"))

    # 📰 뉴스: 이번 주(최대 5일) 제목 중 영향 큰 것
    def top_news(query, n):
        try:
            items = _google_news(query, hours=24 * 5)
        except Exception:
            return []
        seen, out = set(), []
        for it in sorted(items, key=_news_score, reverse=True):
            key = re.sub(r"[^가-힣A-Za-z0-9]", "", it["title"])[:22]
            if not it["title"] or key in seen:
                continue
            seen.add(key)
            out.append(it)
            if len(out) >= n:
                break
        return out

    kn = top_news("코스피 OR 코스닥 OR 외국인 순매도 OR 외국인 순매수 OR 반도체 OR 금리", 6)
    parts.append("🇰🇷 국내뉴스\n" + ("\n".join(f"· {x['title']} ({x['source']})" for x in kn) if kn else "· (없음)"))
    wl = []
    for label, q in NEWS_REGIONS:
        if label.startswith("🇰🇷"):
            continue
        for it in top_news(q, 1):
            wl.append(f"· {label} {it['title']} ({it['source']})")
    parts.append("🌍 해외뉴스\n" + ("\n".join(wl) if wl else "· (없음)"))
    parts.append("※ 숫자는 시세 기준, 뉴스는 이번 주 제목 중 영향 큰 것만 모았어요. 다음 주 일정은 월요일 장전 리포트에서.")
    return "\n\n".join(parts), now.strftime("%Y-%m-%d")


def _run_weekly_review():
    now = now_kst()
    if now.weekday() != 4 or not ((22, 0) <= (now.hour, now.minute) <= (22, 40)):
        return
    day = now.strftime("%Y-%m-%d")
    if _brief_already_sent("weekly_review", day):
        return
    text, _ = build_weekly_review()
    tok, chat = bot("report")
    if send_telegram(text, tok, chat):
        _mark_brief_sent("weekly_review", day)


@app.get("/api/weekly-review")
def weekly_review_endpoint(send: bool = False):
    text, d = build_weekly_review()
    sent = False
    if send:
        tok, chat = bot("report")
        sent = send_telegram(text, tok, chat)
    return {"ok": True, "sent": sent, "text": text}


@app.get("/api/ping")
def ping():
    return {"ok": True}
def _kis_daily_candles(code: str, start: str, end: str, market: str = "J") -> list:
    """🛟 한투 일봉 (KRX가 막히거나 점검일 때 대신) — 100개씩 이어 받기 (2026-10-31)"""
    s_ymd, e_ymd = start.replace("-", ""), end.replace("-", "")
    out, cur = {}, e_ymd
    for _ in range(16):  # 100봉씩 최대 1,600봉 (역배정초 볼린저 300용 약 4년)
        j = kis_get("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
                    {"FID_COND_MRKT_DIV_CODE": market, "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": s_ymd,
                     "FID_INPUT_DATE_2": cur, "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
        rows = [r for r in (j.get("output2") or []) if r.get("stck_bsop_date") and r.get("stck_clpr")]
        if not rows:
            break
        for r in rows:
            d = r["stck_bsop_date"]
            if s_ymd <= d <= e_ymd:
                out[d] = {"date": f"{d[:4]}-{d[4:6]}-{d[6:]}", "open": float(r.get("stck_oprc") or 0),
                          "high": float(r.get("stck_hgpr") or 0), "low": float(r.get("stck_lwpr") or 0),
                          "close": float(r["stck_clpr"]), "volume": float(r.get("acml_vol") or 0),
                          "amount": float(r.get("acml_tr_pbmn") or 0)}
        oldest = min(r["stck_bsop_date"] for r in rows)
        if oldest <= s_ymd or len(rows) < 90:
            break
        cur = (datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
        time.sleep(0.1)
    return [out[k] for k in sorted(out)]


def _stock_ohlcv_base(code: str, start: str, end: str):
    """
    목표가 매핑(패턴분석) 캔들차트용 — 정밀 일봉 시가/고가/저가/종가/거래량.
    pykrx(KRX 공식 데이터)로 가져오므로 키움/한투 로그인과 무관하게 항상 정확함.
    start/end: "YYYY-MM-DD" 형식.
    """
    def kis_fallback(why: str):
        # 🛟 KRX가 안 되면 한투 일봉으로 (밤·주말 점검, KRX 차단 때도 일봉 세력평단이 계속 나오게) — 2026-10-31
        try:
            cs = _kis_daily_candles(code, start, end)
            if cs:
                print(f"[stock_ohlcv] {code} KRX 실패({why[:60]}) → 한투 일봉 {len(cs)}개로 대신")
                return {"ok": True, "code": code, "candles": cs, "source": "KIS"}
        except Exception as e2:
            print(f"[stock_ohlcv] 한투 대신 받기도 실패: {e2}")
        return {"ok": False, "error": why}

    if stock is None:
        return kis_fallback("pykrx를 사용할 수 없습니다")
    try:
        start_ymd = start.replace("-", "")
        end_ymd = _krx_ready_end(end.replace("-", ""), 9)  # 장 시작 전엔 오늘 일봉이 없어서 전 거래일까지
        df = _krx_df(stock.get_market_ohlcv_by_date, start_ymd, end_ymd, code, retries=1)
        if df is None or df.empty:
            return kis_fallback(f"{code} 종목의 {start}~{end} 구간 일봉 데이터가 없습니다.")

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
                    "amount": float(row.get("거래대금", 0) or 0),  # 원 단위 (거래대금 기준봉 판정용)
                }
            )
        return {"ok": True, "code": code, "candles": candles}
    except Exception as e:
        return kis_fallback(str(e))
# ══════════════════════════════════════════════════════════════════════════
# 🔀 데이터 원칙 (2026-11-07 JM): PC 켜짐 → 키움 · PC 꺼짐 → 한투/KRX
#    모드: krx = 🕘 정규장만 (기본) · al = 🌙 통합 (KRX+NXT, 장 전·장 후 포함)
# ══════════════════════════════════════════════════════════════════════════
_kw_asked: dict = {}   # (code, mode) → 마지막으로 PC에 맡긴 시각 (같은 걸 계속 맡기지 않게)


def _pc_alive() -> bool:
    return time.time() - _bpc.get("alive_at", 0) < 30


def _kw_daily_fresh_sec() -> int:
    """키움 일봉을 얼마나 오래 써도 되나 — 장중엔 짧게(오늘 봉이 계속 바뀜), 장 끝나면 길게"""
    n = now_kst()
    hm = n.hour * 100 + n.minute
    return 90 if (n.weekday() < 5 and 800 <= hm <= 2005) else 3600


def _kiwoom_daily_ask(code: str, mode: str):
    """PC에 일봉을 '맡기기만' 하고 기다리지 않음 (화면은 일단 한투/KRX로 바로 보여주고, 다음부터 키움)"""
    k = (code, mode)
    if time.time() - _kw_asked.get(k, 0) < 60 or not _pc_alive():
        return
    _kw_asked[k] = time.time()
    with _bpc_lock:
        _bpc["tasks"].append({"kind": "daily_al", "codes": [code], "suffix": "" if mode == "krx" else "_AL"})


@app.get("/api/data-source")
def data_source_status():
    """지금 어디서 데이터를 받는지 — 화면 배지용 (🟢 키움 / ⚪ 한투)"""
    return {"ok": True, "pc_alive": _pc_alive(), "source": "kiwoom" if _pc_alive() else "kis",
            "label": "🟢 키움" if _pc_alive() else "⚪ 한투", "pc_seen_sec": round(time.time() - _bpc.get("alive_at", 0))}


@app.get("/api/stock-ohlcv")
def stock_ohlcv_endpoint(code: str, start: str, end: str, mode: str = "krx", wait: int = 0):
    """
    일봉 — mode=krx 🕘 정규장만 (기본) / mode=al 🌙 통합(KRX+NXT)
    PC 켜짐 → 키움 일봉(최근 약 600봉)을 덮어씀 · 그보다 오래된 구간은 KRX/한투로 채움
    PC 꺼짐 → krx: KRX(pykrx) → 한투 J / al: 한투 UN
    wait=1이면 키움 일봉이 올 때까지 최대 40초 기다림 (연구·검증용)
    """
    mode = "al" if mode == "al" else "krx"
    if mode == "krx":
        base = _stock_ohlcv_base(code, start, end)
    else:
        try:
            cs = _kis_daily_candles(code, start, end, "UN")
            base = {"ok": bool(cs), "code": code, "candles": cs, "source": "KIS",
                    **({} if cs else {"error": "통합 일봉을 못 받았어요"})}
        except Exception as e:
            base = {"ok": False, "error": str(e)}
    store = _kk_daily if mode == "krx" else _kw_daily
    hit = store.get(code)
    if _pc_alive() and (not hit or time.time() - hit[0] > _kw_daily_fresh_sec()):
        if wait:
            _kiwoom_daily_al([code], wait=40, krx_only=(mode == "krx"))
            hit = store.get(code)
        else:
            _kiwoom_daily_ask(code, mode)
    src = base.get("source") or ("KRX" if base.get("ok") else "")
    if hit and hit[1]:
        merged = {c["date"]: c for c in (base.get("candles") or [])}
        n_kw = 0
        for c in hit[1]:
            if start <= c["date"] <= end:
                merged[c["date"]] = c
                n_kw += 1
        if n_kw:
            return {"ok": True, "code": code, "candles": [merged[k] for k in sorted(merged)], "mode": mode,
                    "source": "키움" if n_kw == len(merged) else f"키움+{src or '한투'}",
                    "kiwoom_age_sec": round(time.time() - hit[0])}
    base["mode"] = mode
    base["source"] = src or "한투"
    return base


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
def stock_minute_ohlcv_endpoint(code: str, market: str = "J", start_time: str = "", mode: str = ""):
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
    if mode:  # 🔀 2026-11-07: mode=krx 🕘 정규장만 / al 🌙 통합
        market = "UN" if mode == "al" else "J"
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
            "mode": "al" if market == "UN" else "krx",
            "source": "한투",
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


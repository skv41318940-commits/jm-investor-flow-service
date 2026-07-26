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
from datetime import datetime, timedelta

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pykrx import stock
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


@app.get("/api/kis-program-daily-debug")
def kis_program_daily_debug(code: str):
    """디버그 전용: '종목별 프로그램매매추이(일별)' 원본 응답 그대로 반환."""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"ok": False, "error": "KIS_APP_KEY / KIS_APP_SECRET 환경변수가 설정되어 있지 않습니다."}
    try:
        from datetime import datetime as _dt
        today_str = _dt.now().strftime("%Y%m%d")
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/program-trade-by-stock-daily",
            tr_id="FHPPG04650201",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": today_str},
        )
        print(f"[kis_program_daily_debug] code={code} raw={data}")
        return {"ok": True, "raw": data}
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

    trade_date = datetime.now().strftime("%Y-%m-%d")
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
    today = datetime.now()
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

    d = datetime.now()
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
    today = datetime.now()
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

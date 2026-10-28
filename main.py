"""
main.py
──────────────────────────────────────────────────────
세력 평단 트래커 메인 앱

레이아웃:
  ┌─────────────────────────────────────────────┐
  │  상단: 종목 검색 + LIVE 시작/종료            │
  ├───────────────────┬─────────────────────────┤
  │  중단 좌: 핵심 지표│  중단 우: 조건검색 스캐너│
  ├───────────────────┴─────────────────────────┤
  │  하단: 3분봉 차트 (종가+평단+순매수증감)      │
  └─────────────────────────────────────────────┘

실행: python main.py  (32bit Python 필수)
"""

import sys
import threading
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QHBoxLayout, QSplitter,
    QFrame, QLabel, QLineEdit, QPushButton,
    QMessageBox, QStatusBar
)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtGui import QFont, QColor
import pyqtgraph as pg

from kiwoom_api      import KiwoomAPI
from data_engine     import DataEngine
from stats_widget    import StatsWidget
from scanner_widget  import ScannerWidget
from market_map_widget import MarketMapWidget
from daily_widget    import DailyWidget
from minute_widget   import MinuteWidget
from info_widget     import InfoWidget
from kis_api         import KisAPI
from nxt_widget      import NxtWidget

# ── 색상 ──────────────────────────────────────────────
C_BG     = "#1e1e1e"
C_PANEL  = "#2a2a2a"
C_BORDER = "#3a3a3a"
C_TEXT   = "#f0f0f0"
C_MUTED  = "#888888"
C_RED    = "#ff4444"
C_GREEN  = "#44cc88"


class MainWindow(QMainWindow):
    sig_sync_done = pyqtSignal(dict)
    sig_query_request = pyqtSignal(str, str, str, str, str, str, str)   # code, start, end, mode, time_start, time_end, request_id
    sig_search_request = pyqtSignal(str, str)   # query, request_id
    sig_program_trade_request = pyqtSignal(str, str, str, str)  # code, date_from, date_to, request_id
    def __init__(self, api: KiwoomAPI):
        super().__init__()
        self.api    = api
        self.engine = DataEngine()
        self._kis   = None  # 한투 API (NXT 탭 클릭 시 초기화)

        self._code  = ""
        self._name  = ""
        self._live  = False
        self._scanner_codes = set()

        self.setWindowTitle("세력 트래커  |  3분봉 · 세력평단 · 순매수증감")
        self.resize(1440, 880)
        self._apply_style()
        self._build_ui()
        self._connect_signals()

        # 🦀 꽃게 검색기 PC 연동 (작은 꽃게·큰 꽃게 실시간 → JM 서버)
        try:
            from crab_pc import CrabPCWatcher
            self.crab = CrabPCWatcher(api)
        except Exception as e:
            self.crab = None
            print(f"[🦀 PC] 시작 실패: {e}")

        # 조건식 로드
        QTimer.singleShot(800, self._load_conditions)
        self._start_live_bridge()
        self.sig_sync_done.connect(self._on_sync_done)
        self.sig_query_request.connect(self._on_query_request)
        self._query_events = {}
        self._query_results = {}
        self.sig_search_request.connect(self._on_search_request)
        self._search_events = {}
        self._search_results = {}
        self.sig_program_trade_request.connect(self._on_program_trade_request)
        self._program_trade_events = {}
        self._program_trade_results = {}

    def _start_live_bridge(self):
        """웹사이트(폰 포함)에서 실시간 데이터를 읽어갈 수 있는 로컬 API 서버 시작"""
        try:
            from live_server import start_live_server
            start_live_server(
                self._live_snapshot,
                get_market_map=self.market_map.get_snapshot,
                query_avg=self._query_avg_sync,
                search_stock=self._search_stock_sync,
                get_scanner=self.scanner.get_snapshot,
                get_sector_stocks=self.market_map.get_stocks_for_sector,
                sync_fundamentals_one=self._sync_fundamentals_one,
                sync_investor_flow=self._sync_investor_flow_one,
                sync_program_trade=self._sync_program_trade_one,
                port=8899,
            )
        except Exception as e:
            print(f"[live_bridge] 시작 실패 (fastapi/uvicorn 설치 필요할 수 있음): {e}")

    def _live_snapshot(self) -> dict:
        """live_server가 /api/live 요청받을 때마다 호출 - 현재 LIVE 종목 스냅샷 반환"""
        from datetime import datetime
        if not self._live or not self._code:
            return {"live": False, "message": "LIVE 감시 중인 종목이 없습니다"}

        stats = self.engine.get_stats()
        candles = self.engine.get_candles_for_chart()
        candle_list = [
            {
                "timestamp": c.timestamp, "open": c.open, "high": c.high,
                "low": c.low, "close": c.close, "volume": c.volume,
                "net_buy": c.net_buy, "cum_net_buy": c.cum_net_buy,
                "avg_price": c.avg_price, "avg_sell_price": c.avg_sell_price,
            }
            for c in candles
        ]
        return {
            "live": True,
            "code": self._code,
            "name": self._name,
            "current_price": self.engine.current_price,
            "stats": stats,
            "candles": candle_list,
            "updated_at": datetime.now().strftime("%H:%M:%S"),
        }

    # ════════════════════════════════════════════════════════
    #  스타일
    # ════════════════════════════════════════════════════════
    def _apply_style(self):
        self.setStyleSheet(f"""
        * {{ font-family:'Malgun Gothic','맑은 고딕',sans-serif; }}
        QMainWindow, QWidget {{ background:{C_BG}; color:{C_TEXT}; font-size:13px; }}
        QSplitter::handle {{ background:{C_BORDER}; }}
        QStatusBar {{ background:{C_PANEL}; color:{C_MUTED}; font-size:11px; border-top:1px solid {C_BORDER}; }}
        QLineEdit {{
            background:#333; border:1px solid {C_BORDER};
            border-radius:4px; padding:4px 8px; color:{C_TEXT};
        }}
        QLineEdit:focus {{ border-color:#1f6feb; }}
        """)

    # ════════════════════════════════════════════════════════
    #  UI 구성
    # ════════════════════════════════════════════════════════
    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── 상단 컨트롤 바 ─────────────────────────────────
        root.addWidget(self._build_topbar())

        # ── 중단 (좌: 지표 / 중: 시장 지도 / 우: 스캐너) ──────
        mid_split = QSplitter(Qt.Horizontal)
        mid_split.setHandleWidth(3)
        mid_split.setFixedHeight(420)

        self.stats      = StatsWidget()
        self.market_map = MarketMapWidget()
        self.market_map.sig_stock_click.connect(self._on_scanner_select)
        self.scanner = ScannerWidget()
        self.scanner.btn_search.clicked.connect(self._run_condition)
        self.scanner.btn_refresh.clicked.connect(self._load_conditions)
        self.scanner.btn_watch.clicked.connect(self._on_watch_toggle)
        self.scanner.sig_select.connect(self._on_scanner_select)
        self.scanner.sig_perf_date_request.connect(self._on_scanner_perf_request)

        mid_split.addWidget(self.stats)
        mid_split.addWidget(self.market_map)
        mid_split.addWidget(self.scanner)
        mid_split.setSizes([230, 650, 660])
        root.addWidget(mid_split)

        # 구분선
        sep = QFrame()
        sep.setFixedHeight(2)
        sep.setStyleSheet(f"background:{C_BORDER};")
        root.addWidget(sep)

        # ── 하단 탭 (3분봉 / 일봉) ────────────────────────
        from PyQt5.QtWidgets import QTabWidget
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet(f"""
            QTabWidget::pane {{
                border:1px solid {C_BORDER}; background:{C_BG};
            }}
            QTabBar::tab {{
                background:{C_PANEL}; color:{C_MUTED};
                border:1px solid {C_BORDER}; border-bottom:none;
                padding:6px 20px; margin-right:2px;
                border-top-left-radius:4px; border-top-right-radius:4px;
            }}
            QTabBar::tab:selected {{
                background:{C_BG}; color:{C_TEXT};
                border-bottom:2px solid #1f6feb;
            }}
        """)

        self.daily  = DailyWidget()
        self.minute = MinuteWidget()
        self.info   = InfoWidget(api=self.api)
        self.daily.sig_request.connect(self._on_daily_request)
        self.daily.sig_prog_request.connect(self._on_prog_api_request)
        self.daily.sig_prog_csv.connect(self._on_prog_csv_request)
        self.minute.sig_request.connect(self._on_minute_request)
        self.minute.sig_prog_request.connect(self._on_minute_prog_api_request)
        self.minute.sig_prog_csv.connect(self._on_minute_prog_csv_request)

        self.nxt = NxtWidget(kis=None)  # kis는 탭 클릭 시 전달
        self.tabs.addTab(self.daily,  "📊  일봉 세력평단")
        self.tabs.addTab(self.minute, "🕯  분봉 분석")
        self.tabs.addTab(self.nxt,    "🌙  NXT 한투 전용 분봉분석")
        self.tabs.addTab(self.info,   "🔎  종목 정보")
        root.addWidget(self.tabs, stretch=1)

        # 상태바
        self.setStatusBar(QStatusBar())
        self.statusBar().showMessage("키움 OpenAPI 로그인 완료  |  종목코드를 입력하고 LIVE 시작을 누르세요")

    def _build_topbar(self) -> QFrame:
        bar = QFrame()
        bar.setFixedHeight(52)
        bar.setStyleSheet(f"background:{C_PANEL}; border-bottom:1px solid {C_BORDER};")
        hl = QHBoxLayout(bar)
        hl.setContentsMargins(12, 0, 12, 0)
        hl.setSpacing(8)

        def lbl(txt):
            l = QLabel(txt)
            l.setStyleSheet(f"color:{C_MUTED}; font-size:12px;")
            return l

        # 종목코드 입력
        self.input_code = QLineEdit()
        self.input_code.setPlaceholderText("종목코드  예) 005930")
        self.input_code.setFixedWidth(150)
        self.input_code.returnPressed.connect(self._on_code_enter)

        # 종목명 표시
        self.lbl_name = QLabel("")
        self.lbl_name.setStyleSheet("font-size:15px; font-weight:bold; margin-left:4px; min-width:120px;")

        # 현재가
        self.lbl_price = QLabel("—")
        self.lbl_price.setStyleSheet(f"font-size:18px; font-weight:bold; color:{C_TEXT}; margin-left:10px;")

        # 등락
        self.lbl_chg = QLabel("")
        self.lbl_chg.setStyleSheet(f"font-size:14px; margin-left:6px;")

        # LIVE 버튼
        self.btn_live = QPushButton("▶  LIVE 시작")
        self.btn_live.setFixedSize(120, 34)
        self.btn_live.setStyleSheet(self._live_btn_style(False))
        self.btn_live.clicked.connect(self._on_live_toggle)

        # Supabase 동기화 버튼 (웹사이트 관심종목 세력평단 채우기)
        self.btn_sync = QPushButton("☁  Supabase 동기화")
        self.btn_sync.setFixedSize(150, 34)
        self.btn_sync.setStyleSheet(
            "QPushButton{background:#1a6b6b;border:none;border-radius:6px;"
            "color:white;font-size:12px;font-weight:bold;}"
            "QPushButton:hover{background:#1f8080;}"
        )
        self.btn_sync.clicked.connect(self._on_supabase_sync)

        # 계정 정보
        uid   = self.api.get_user_id()
        accts = self.api.get_account_list()
        acct_str = accts[0] if accts else "—"
        lbl_acct = QLabel(f"ID: {uid}  |  계좌: {acct_str}")
        lbl_acct.setStyleSheet(f"color:{C_MUTED}; font-size:11px; margin-left:10px;")

        hl.addWidget(lbl("종목코드"))
        hl.addWidget(self.input_code)
        hl.addWidget(self.lbl_name)
        hl.addWidget(self.lbl_price)
        hl.addWidget(self.lbl_chg)
        hl.addStretch()
        hl.addWidget(lbl_acct)
        hl.addWidget(self.btn_sync)
        hl.addWidget(self.btn_live)
        return bar

    @staticmethod
    def _live_btn_style(is_live: bool) -> str:
        if is_live:
            return """QPushButton {
                background:#7a1515; border:none; border-radius:5px;
                color:white; font-size:13px; font-weight:bold;
            }
            QPushButton:hover { background:#9a2020; }"""
        else:
            return """QPushButton {
                background:#155a2a; border:none; border-radius:5px;
                color:white; font-size:13px; font-weight:bold;
            }
            QPushButton:hover { background:#1e7a3a; }"""

    # ════════════════════════════════════════════════════════
    #  시그널 연결
    # ════════════════════════════════════════════════════════
    def _connect_signals(self):
        self.api.sig_chejan.connect(self._on_chejan)
        self.tabs.currentChanged.connect(self._on_tab_changed)
        self.api.sig_condition_list.connect(self._on_condition_list)
        self.api.sig_condition_result.connect(self._on_condition_result)
        self.api.sig_real_condition.connect(self._on_real_condition)
        self.engine.on_vol_high   = self._on_vol_high
        self.engine.on_money_flow = self._on_money_flow

    # ════════════════════════════════════════════════════════
    #  LIVE 시작 / 종료
    # ════════════════════════════════════════════════════════
    def _on_code_enter(self):
        """종목코드 입력 후 Enter → 종목명/탭 연동 + LIVE 시작"""
        code = self.input_code.text().strip().zfill(6)
        if not code:
            return
        self.daily.set_code(code)
        self.minute.set_code(code)
        name = self.api.get_stock_name(code)
        self.info.load(code, name)
        self.info.load_kiwoom_info(code)
        if hasattr(self, "nxt"): self.nxt.set_code(code, name)
        self._on_live_toggle()

    def _on_live_toggle(self):
        if not self._live:
            self._live = True
            self.btn_live.setText("■  LIVE 종료")
            self.btn_live.setStyleSheet(self._live_btn_style(True))
            self.statusBar().showMessage(
                "LIVE 중  |  스캐너/포착 실시간 수신 중  |  일봉/분봉은 조회 버튼으로 확인하세요"
            )
        else:
            self.api.unreg_realtime()
            self._live = False
            self._code = ""
            self._name = ""
            self.btn_live.setText("▶  LIVE 시작")
            self.btn_live.setStyleSheet(self._live_btn_style(False))
            self.statusBar().showMessage("LIVE 종료")

    # ════════════════════════════════════════════════════════
    #  실시간 체결 수신 → 엔진 → 화면 갱신
    # ════════════════════════════════════════════════════════
    def _on_chejan(self, data: dict):
        code = data.get("code")
        if not code:
            return

        # 조건검색 등록 종목이면 틱 저장
        if code in self._scanner_codes:
            self._save_tick_for(code, data)

        # LIVE 중인 종목이면 엔진 + 화면 갱신
        if not self._live or code != self._code:
            return

        # 데이터 엔진에 틱 공급
        self.engine.on_tick(data)

        # LIVE 종목 틱 저장
        self._save_tick(data)

        # 현재가 상단 표시
        price = data["price"]
        rate  = data["chg_rate"]
        col   = C_RED if rate >= 0 else "#4488ff"
        self.lbl_price.setText(f"{price:,} 원")
        sign = "+" if rate >= 0 else ""
        self.lbl_chg.setText(f"{sign}{rate:.2f}%")
        self.lbl_chg.setStyleSheet(f"font-size:14px; margin-left:6px; color:{col};")

        # 지표 창 갱신
        stats = self.engine.get_stats()
        stats["stock_name"] = self._name
        stats["stock_code"] = self._code
        self.stats.update_stats(stats)

        # 스캐너 가격 갱신
        self.scanner.update_price(self._code, price, rate)

        # 차트 갱신 (3분봉)
        candles = self.engine.get_candles_for_chart()

    # ════════════════════════════════════════════════════════
    #  조건검색
    # ════════════════════════════════════════════════════════
    def _load_conditions(self):
        self.statusBar().showMessage("조건식 목록 불러오는 중…")
        self.api.load_conditions()

    def _on_condition_list(self, cond_list: list):
        self.scanner.set_conditions(cond_list)
        if getattr(self, "crab", None):
            QTimer.singleShot(1500, lambda: self.crab.start(cond_list))  # 🦀 작은 꽃게·큰 꽃게 감시 시작
        self.statusBar().showMessage(f"조건식 {len(cond_list)}개 로드 완료  |  조건식을 선택 후 검색 버튼을 누르세요")

    def _run_condition(self):
        idx, name = self.scanner.get_selected()
        if not idx:
            QMessageBox.information(self, "알림", "조건식을 선택하세요.")
            return
        self.api.search_condition(idx, name)
        self.statusBar().showMessage(f"조건검색 실행 중: [{name}]")

    def _on_watch_toggle(self):
        """감시 시작/중지"""
        if self.scanner.is_watching():
            self.statusBar().showMessage("감시 중지 — 종목 목록은 유지됩니다")
        else:
            idx, name = self.scanner.get_selected()
            if not idx:
                QMessageBox.information(self, "알림", "먼저 조건식을 선택하세요.")
                self.scanner._on_watch_toggle()
                return
            self.api.start_realtime_condition(idx, name)
            self.statusBar().showMessage(f"감시 시작: [{name}]")

    def _on_condition_result(self, items: list):
        self.scanner.set_results(items)
        # 검색 결과 종목 전부 실시간 등록 + 틱 저장 시작
        for item in items:
            code = item["code"]
            self._scanner_codes.add(code)
            self.api.reg_realtime_add(code)
        self.statusBar().showMessage(
            f"조건검색 결과: {len(items)}종목 — 전체 틱 저장 시작"
        )

    def _on_scanner_select(self, code: str, name: str):
        """스캐너/업종 클릭 → 종목코드 입력창 업데이트"""
        self.input_code.setText(code)
        self.lbl_name.setText(name)
        self._code = code
        self._name = name

        self.daily.set_code(code)
        self.minute.set_code(code)
        self.info.load(code, name)
        self.info.load_kiwoom_info(code)
        if hasattr(self, "nxt"): self.nxt.set_code(code, name)

        self.statusBar().showMessage(
            f"[{code}] {name} 선택됨  |  일봉/분봉 탭에서 조회 버튼을 누르세요"
        )

    def _on_real_condition(self, code: str, event_type: str, cond_name: str = ""):
        name = self.api.get_stock_name(code)

        # 스캐너 탭 (큰형님 자전거 등 사용자가 선택한 조건식)
        _, selected_name = self.scanner.get_selected()
        if not cond_name or cond_name in selected_name or selected_name in cond_name:
            self.scanner.on_real_condition(code, event_type, name)

        if event_type == "I":
            self._scanner_codes.add(code)
            self.api.reg_realtime_add(code)
            self.statusBar().showMessage(f"편입: [{code}] {name} [{cond_name}]")
            self._log_scanner_capture(code, name)
        # 이탈해도 실시간 해제 안 함 → 틱 계속 저장

    def _load_today_data(self, code: str):
        """
        LIVE 시작 시 오늘 장중 분봉 데이터 자동 로드
        → 핵심 지표 즉시 계산 + 3분봉 차트 즉시 표시
        """
        from datetime import datetime
        from data_engine import Candle3Min
        today = datetime.now().strftime("%Y%m%d")

        self.statusBar().showMessage(f"오늘 분봉 데이터 로드 중... [{code}]")

        # 키움 API로 오늘 데이터 로드
        result  = self.api.request_minute_history(
            code, today, "3", False, "0900", "1530"
        )
        candles = result.get("candles", [])

        if not candles:
            self.statusBar().showMessage(
                f"LIVE 중  |  [{code}] {self._name}  |  실시간 체결 수신 중..."
            )
            return

        # ── 매수/매도 체결량 지원 여부 확인 ──────────────
        has_buysell = any(c["buy_qty"] > 0 or c["sell_qty"] > 0 for c in candles)

        # ── 추정값 포함 candles 재계산 ───────────────────
        enriched = []
        for c in candles:
            vol    = c["volume"]
            open_p = c.get("open", 0)

            if has_buysell:
                buy_qty  = c["buy_qty"]
                sell_qty = c["sell_qty"]
                net_buy  = c["net_buy"]
            else:
                # 시가 vs 종가로 순매수 추정
                if open_p > 0 and c["close"] >= open_p:
                    ratio   = min((c["close"] - open_p) / open_p * 10, 0.5)
                    net_buy = int(vol * ratio)
                elif open_p > 0:
                    ratio   = min((open_p - c["close"]) / open_p * 10, 0.5)
                    net_buy = -int(vol * ratio)
                else:
                    net_buy = 0
                buy_qty  = max(0, (vol + net_buy) // 2)
                sell_qty = max(0, (vol - net_buy) // 2)

            enriched.append({**c,
                "buy_qty":  buy_qty,
                "sell_qty": sell_qty,
                "net_buy":  net_buy,
            })

        # ── 핵심 지표 계산 ──────────────────────────────
        self.engine.reset()
        for c in enriched:
            self.engine.on_tick({
                "price":     c["close"],
                "volume":    c["volume"],
                "buy_qty":   c["buy_qty"],
                "sell_qty":  c["sell_qty"],
                "net_buy":   c["net_buy"],
                "chg_rate":  0.0,
                "chg_price": 0,
                "exec_time": c["time"].replace(":", "") + "00",
            })
        stats = self.engine.get_stats()
        stats["stock_name"] = self._name
        stats["stock_code"] = self._code
        self.stats.update_stats(stats)

        # ── 3분봉 차트 즉시 표시 ───────────────────────
        candle_objs = []
        cum_buy_val = cum_buy_vol = 0
        cum_sell_val = cum_sell_vol = 0
        cum_net = 0

        for c in enriched:
            if c["buy_qty"] > 0:
                cum_buy_val  += c["close"] * c["buy_qty"]
                cum_buy_vol  += c["buy_qty"]
            if c["sell_qty"] > 0:
                cum_sell_val += c["close"] * c["sell_qty"]
                cum_sell_vol += c["sell_qty"]
            cum_net += c["net_buy"]

            candle_objs.append(Candle3Min(
                timestamp      = c["time"],
                close          = c["close"],
                volume         = c["volume"],
                buy_qty        = c["buy_qty"],
                sell_qty       = c["sell_qty"],
                net_buy        = c["net_buy"],
                cum_net_buy    = cum_net,
                avg_price      = round(cum_buy_val / cum_buy_vol) if cum_buy_vol else 0,
                avg_sell_price = round(cum_sell_val / cum_sell_vol) if cum_sell_vol else 0,
            ))
        self.lbl_price.setText(f"{enriched[-1]['close']:,} 원")

        self.statusBar().showMessage(
            f"LIVE 중  |  [{code}] {self._name}  |  "
            f"오늘 {len(candles)}봉 로드 완료  |  실시간 체결 수신 중..."
        )

    def _on_minute_request(self, code: str, date: str, interval: str, is_nxt: bool = False):
        """과거 분봉 세력평단 조회
        NXT ON  → 한투 API (08:00~20:00)
        NXT OFF → 1순위 틱데이터, 2순위 키움 API (정규장)
        """
        t_start, t_end = self.minute.get_time_range()
        name = self.api.get_stock_name(code)

        # 키움 API로 전체 조회
        self.statusBar().showMessage(
            f"분봉 조회 중... [{code}] {date} {interval}분봉  "
            f"{t_start[:2]}:{t_start[2:]}~{t_end[:2]}:{t_end[2:]}"
        )
        result  = self.api.request_minute_history(
            code, date, interval, False, t_start, t_end
        )
        candles = result.get("candles", [])

        self.minute.update_chart(candles, name)

        if candles:
            self.engine.load_candles(candles)
            stats = self.engine.get_stats()
            stats["stock_name"] = name
            stats["stock_code"] = code
            self.stats.update_stats(stats)
            final = candles[-1]
            self.lbl_name.setText(name)
            self.lbl_price.setText(f"{final['close']:,} 원")
            self._code = code
            self._name = name
            self.input_code.setText(code)
            if hasattr(self, "nxt"): self.nxt.set_code(code, name)

        self.statusBar().showMessage(
            f"[키움] 분봉 조회 완료: [{code}] {name}  {interval}분봉  {len(candles)}봉"
        )
    def _save_tick(self, data: dict):
        """LIVE 종목 틱 저장"""
        self._save_tick_for(self._code, data)

    def _save_tick_for(self, code: str, data: dict):
        """특정 종목 틱 데이터를 날짜별 CSV로 저장"""
        import os, csv
        from datetime import datetime
        try:
            tick_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tick_data")
            os.makedirs(tick_dir, exist_ok=True)

            today = datetime.now().strftime("%Y%m%d")
            path  = os.path.join(tick_dir, f"{today}_{code}.csv")
            is_new = not os.path.exists(path)

            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if is_new:
                    writer.writerow(["time","price","volume","buy_qty","sell_qty","net_buy"])
                writer.writerow([
                    data.get("exec_time", ""),
                    data.get("price",    0),
                    data.get("volume",   0),
                    data.get("buy_qty",  0),
                    data.get("sell_qty", 0),
                    data.get("net_buy",  0),
                ])
        except Exception:
            pass

    def _get_tick_candles(self, code: str, date: str,
                          interval: int, t_start: str, t_end: str) -> list:
        """틱 CSV → 분봉 집계 (리스트 반환, UI 업데이트 안 함)"""
        import os, csv
        from collections import defaultdict
        try:
            tick_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tick_data")
            path = os.path.join(tick_dir, f"{date}_{code}.csv")
            if not os.path.exists(path):
                return []

            ts_int = int(t_start)
            te_int = int(t_end)
            ticks = []
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        t = row["time"].strip()
                        if len(t) < 6:
                            continue
                        hh = int(t[0:2])
                        mm = int(t[2:4])
                        t_int = hh * 100 + mm
                        if t_int < ts_int or t_int > te_int:
                            continue
                        ticks.append({
                            "hh": hh, "mm": mm,
                            "price":    abs(int(row["price"]    or 0)),
                            "volume":   abs(int(row["volume"]   or 0)),
                            "buy_qty":  abs(int(row["buy_qty"]  or 0)),
                            "sell_qty": abs(int(row["sell_qty"] or 0)),
                            "net_buy":  int(row["net_buy"] or 0),
                        })
                    except Exception:
                        continue

            if not ticks:
                return []

            slots = defaultdict(list)
            for t in ticks:
                slot_mm = (t["mm"] // interval) * interval
                key = f"{t['hh']:02d}:{slot_mm:02d}"
                slots[key].append(t)

            candles = []
            for slot_key in sorted(slots.keys()):
                st = slots[slot_key]
                buy_qty  = sum(x["buy_qty"]  for x in st)
                sell_qty = sum(x["sell_qty"] for x in st)
                candles.append({
                    "time":      slot_key,
                    "close":     st[-1]["price"],
                    "open":      st[0]["price"],
                    "volume":    sum(x["volume"] for x in st),
                    "buy_qty":   buy_qty,
                    "sell_qty":  sell_qty,
                    "net_buy":   buy_qty - sell_qty,
                    "avg_price": 0,
                })
            return candles
        except Exception:
            return []

    def _merge_candles(self, api_candles: list, tick_candles: list) -> list:
        """키움 API 봉 + 틱 봉 병합 (틱 봉 우선 - 정확한 순매수)"""
        if not api_candles:
            return tick_candles
        if not tick_candles:
            return api_candles

        # 틱 봉의 시간 범위 확인
        tick_times = {c["time"] for c in tick_candles}
        tick_start = min(tick_times)

        # API 봉 중 틱 시작 이전 봉만 유지 + 틱 봉 추가
        merged = [c for c in api_candles if c["time"] < tick_start]
        merged.extend(tick_candles)

        # 세력평단 누적 재계산
        cum_val, cum_vol = 0, 0
        for c in merged:
            cum_val += c["close"] * c["volume"]
            cum_vol += c["volume"]
            c["avg_price"] = round(cum_val / cum_vol) if cum_vol else 0

        return merged

    def _load_tick_to_minute(self, code: str, date: str):
        """저장된 틱 CSV → 분봉 집계 → 분봉 분석 탭에 표시"""
        import os, csv
        try:
            tick_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tick_data")
            path = os.path.join(tick_dir, f"{date}_{code}.csv")
            if not os.path.exists(path):
                return False

            interval = int(self.minute.combo.currentText())
            t_start, t_end = self.minute.get_time_range()  # "HHMM" 형식
            ts_int = int(t_start)
            te_int = int(t_end)

            ticks = []
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        t = row["time"].strip()
                        if len(t) < 6:
                            continue
                        hh = int(t[0:2])
                        mm = int(t[2:4])
                        # 시간 범위 필터
                        t_int = hh * 100 + mm
                        if t_int < ts_int or t_int > te_int:
                            continue
                        ticks.append({
                            "hh":       hh,
                            "mm":       mm,
                            "price":    abs(int(row["price"]   or 0)),
                            "volume":   abs(int(row["volume"]  or 0)),
                            "buy_qty":  abs(int(row["buy_qty"] or 0)),
                            "sell_qty": abs(int(row["sell_qty"]or 0)),
                            "net_buy":  int(row["net_buy"] or 0),
                        })
                    except Exception:
                        continue

            if not ticks:
                return False

            # 분봉 집계
            from collections import defaultdict
            slots = defaultdict(list)
            for t in ticks:
                slot_mm = (t["mm"] // interval) * interval
                key = f"{t['hh']:02d}:{slot_mm:02d}"
                slots[key].append(t)

            candles = []
            for slot_key in sorted(slots.keys()):
                st = slots[slot_key]
                close    = st[-1]["price"]
                volume   = sum(t["volume"]   for t in st)
                buy_qty  = sum(t["buy_qty"]  for t in st)
                sell_qty = sum(t["sell_qty"] for t in st)
                net_buy  = buy_qty - sell_qty

                candles.append({
                    "time":      slot_key,
                    "close":     close,
                    "volume":    volume,
                    "buy_qty":   buy_qty,
                    "sell_qty":  sell_qty,
                    "net_buy":   net_buy,
                    "avg_price": 0,
                })

            # 세력평단 누적 계산
            cum_val, cum_vol = 0, 0
            for c in candles:
                cum_val += c["close"] * c["volume"]
                cum_vol += c["volume"]
                c["avg_price"] = round(cum_val / cum_vol) if cum_vol else 0

            name = self.api.get_stock_name(code)
            self.minute.update_chart(candles, name)
            self.statusBar().showMessage(
                f"틱 데이터 로드 완료: [{code}] {date}  {interval}분봉  {len(candles)}봉 (정확한 순매수)"
            )
            return True
        except Exception as e:
            self.statusBar().showMessage(f"틱 로드 오류: {e}")
            return False

    def _on_prog_api_request(self, code: str, date_from: str, date_to: str):
        """opt10060 프로그램매매 API 조회 (일봉 탭)"""
        self.statusBar().showMessage(f"프로그램매매 API 조회 중... [{code}]")
        result = self.api.request_program_trade(code, date_from, date_to)
        data = result.get("data", [])
        self.daily.update_prog_data(data, source="api")
        self.statusBar().showMessage(f"프로그램매매 API 조회 완료: {len(data)}일치")

    def _on_minute_prog_api_request(self, code: str, date_from: str, date_to: str):
        """opt10060 프로그램매매 API 조회 (분봉 탭)"""
        self.statusBar().showMessage(f"프로그램매매 API 조회 중... [{code}]")
        result = self.api.request_program_trade(code, date_from, date_to)
        data = result.get("data", [])
        self.minute.update_prog_data(data, source="api")
        self.statusBar().showMessage(f"프로그램매매 API 조회 완료: {len(data)}일치")

    def _on_minute_prog_csv_request(self, path: str):
        """0778 시간별 CSV → 분봉 집계 → 차트 업데이트"""
        try:
            # euc-kr 인코딩, 헤더 2줄 스킵
            with open(path, encoding='euc-kr') as f:
                lines = f.readlines()

            def to_int(v):
                try:
                    return int(str(v).replace(',','').replace(' ','').replace('"',''))
                except Exception:
                    return 0

            def to_float(v):
                try:
                    return float(str(v).replace(',','').replace(' ','').replace('"',''))
                except Exception:
                    return 0.0

            # 컬럼: 시간,현재가,대비,,등락률,거래량,매도금액,매수금액,순매수금액,순매수증감
            interval = int(self.minute.combo.currentText())  # 분봉 단위

            ticks = []
            for line in lines[2:]:  # 헤더 2줄 스킵
                parts = line.strip().split(',')
                if len(parts) < 10:
                    continue
                time_str  = parts[0].strip().replace('"','')   # HH:MM:SS
                close     = to_int(parts[1])
                volume    = to_int(parts[5])
                sell_amt  = to_int(parts[6])
                buy_amt   = to_int(parts[7])
                net_chg   = to_int(parts[9])

                if close <= 0 or len(time_str) < 5:
                    continue

                hh = int(time_str[0:2])
                mm = int(time_str[3:5])
                ticks.append({
                    "hh": hh, "mm": mm,
                    "close": close,
                    "volume": volume,
                    "buy_amt": buy_amt,
                    "sell_amt": sell_amt,
                    "net_chg": net_chg,
                })

            if not ticks:
                self.statusBar().showMessage("CSV 파싱 오류: 데이터 없음")
                return

            # 시간 오름차순 정렬 (CSV는 최신순)
            ticks.sort(key=lambda x: x["hh"] * 60 + x["mm"])

            # 분봉 집계
            from collections import defaultdict
            slots = defaultdict(list)
            for t in ticks:
                slot_mm = (t["mm"] // interval) * interval
                key = f"{t['hh']:02d}:{slot_mm:02d}"
                slots[key].append(t)

            candles = []
            for slot_key in sorted(slots.keys()):
                slot_ticks = slots[slot_key]
                close   = slot_ticks[-1]["close"]
                volume  = sum(t["volume"]   for t in slot_ticks)
                net_chg = slot_ticks[-1]["net_chg"]  # 마지막 틱의 누적 순매수증감

                candles.append({
                    "time":     slot_key,
                    "close":    close,
                    "volume":   volume,
                    "net_buy":  net_chg,
                    "buy_qty":  0,
                    "sell_qty": 0,
                    "avg_price": 0,
                })

            # avg_price (세력평단) 누적 계산
            cum_val, cum_vol = 0, 0
            for c in candles:
                cum_val += c["close"] * c["volume"]
                cum_vol += c["volume"]
                c["avg_price"] = round(cum_val / cum_vol) if cum_vol else 0

            # 차트 업데이트
            self.minute.update_chart(candles, self.minute.input_code.text())
            self.statusBar().showMessage(
                f"0778 CSV 로드 완료: {len(candles)}봉 ({interval}분봉 집계)"
            )

        except Exception as e:
            self.statusBar().showMessage(f"CSV 파싱 오류: {e}")

    def _on_prog_csv_request(self, path: str):
        """0778 일별 CSV 파싱 후 일봉 차트 업데이트
        형식: 일자,현재가,대비,,등락률,거래량,매도금액,매수금액,순매수금액,순매수증감
        헤더 2줄 + 데이터
        """
        try:
            with open(path, encoding='euc-kr') as f:
                lines = f.readlines()

            def to_int(v):
                try:
                    return int(str(v).strip().replace(',','').replace('"','').replace(' ',''))
                except Exception:
                    return 0

            data = []
            for line in lines[2:]:   # 헤더 2줄 스킵
                parts = line.strip().split(',')
                if len(parts) < 10:
                    continue

                # 날짜: 2026/05/28 → 20260528
                date_raw = parts[0].strip().replace('"','').replace('/','')
                if not date_raw or not date_raw.isdigit() or len(date_raw) != 8:
                    continue

                close = to_int(parts[1])
                if close <= 0:
                    continue

                data.append({
                    "date":     date_raw,
                    "close":    close,
                    "volume":   to_int(parts[5]),
                    "sell_amt": to_int(parts[6]),
                    "buy_amt":  to_int(parts[7]),
                    "net_amt":  to_int(parts[8]),
                    "net_chg":  to_int(parts[9]),
                })

            # CSV는 최신→과거 순이므로 역순으로 (오름차순)
            data.reverse()

            self.daily.update_prog_data(data, source="csv")
            self.statusBar().showMessage(f"일봉 CSV 파싱 완료: {len(data)}일치")
        except Exception as e:
            self.statusBar().showMessage(f"CSV 파싱 오류: {e}")

    def _on_daily_request(self, code: str, date_from: str, date_to: str):
        """일봉 + 주봉 세력평단 동시 조회 + 핵심 지표 업데이트"""
        self.statusBar().showMessage(f"일봉/주봉 조회 중... [{code}] {date_from}~{date_to}")
        name = self.api.get_stock_name(code)

        # 일봉 조회
        daily_result  = self.api.request_daily(code, date_from, date_to)
        daily_candles = daily_result.get("candles", [])

        # 주봉 조회
        weekly_result  = self.api.request_weekly(code, date_from, date_to)
        weekly_candles = weekly_result.get("candles", [])

        self.daily.update_chart(daily_candles, weekly_candles, name)

        # 핵심 지표 업데이트 (일봉 기준)
        if daily_candles:
            # 일봉 → 분봉 형식으로 변환해서 load_candles 사용
            candle_list = [{
                "time":     c["date"],
                "open":     c.get("open", c["close"]),
                "close":    c["close"],
                "volume":   c["volume"],
                "buy_qty":  0,
                "sell_qty": 0,
                "net_buy":  0,
            } for c in daily_candles]
            self.engine.load_candles(candle_list)
            stats = self.engine.get_stats()
            stats["stock_name"] = name
            stats["stock_code"] = code
            self.stats.update_stats(stats)

            # 상단 종목명 + 현재가
            self.lbl_name.setText(name)
            self.lbl_price.setText(f"{daily_candles[-1]['close']:,} 원")
            self._code = code
            self._name = name

        self.statusBar().showMessage(
            f"조회 완료: [{code}] {name}  일봉 {len(daily_candles)}일 / 주봉 {len(weekly_candles)}주"
        )

    # ════════════════════════════════════════════════════════
    #  종료
    # ════════════════════════════════════════════════════════
    def _on_vol_high(self, candle):
        """신고거래량 감지"""
        candle._code = self._code
        candle._name = self._name
        self.statusBar().showMessage(
            f"🔥 신고거래량! [{self._code}] {self._name}  "
            f"{candle.timestamp}  거래량:{candle.volume:,}"
        )

    def _on_money_flow(self, candle, grade):
        """머니플로우 감지"""
        from data_engine import DataEngine
        label, _ = DataEngine.get_money_grade_info(grade)
        candle._code = self._code
        candle._name = self._name
        self.statusBar().showMessage(
            f"💰 머니플로우 {label}! [{self._code}] {self._name}  "
            f"{candle.timestamp}  금액:{candle.money_amount//100000000:.0f}억"
        )

    def _on_tab_changed(self, idx: int):
        """NXT 탭 클릭 시 한투 API 초기화, 벗어나면 해제"""
        nxt_idx = self.tabs.indexOf(self.nxt)
        if idx == nxt_idx:
            # NXT 탭 진입 → 한투 API 초기화
            if self._kis is None:
                self._kis = KisAPI()
                print("[KIS] NXT 탭 진입 → 한투 API 초기화")
            self.nxt.kis = self._kis
            # 현재 종목코드 전달
            if self._code:
                self.nxt.set_code(self._code, self._name)
        else:
            # NXT 탭 벗어남 → 특별히 해제 안 해도 됨 (토큰만 유지)
            pass

    def closeEvent(self, e):
        self.api.unreg_realtime()
        e.accept()

    # ════════════════════════════════════════════════════════
    #  Supabase 동기화 - 웹사이트 관심종목 세력평단 채우기
    # ════════════════════════════════════════════════════════
    def _on_supabase_sync(self):
        """
        주의: 키움 OpenAPI(OCX)는 COM 객체라 메인 스레드에서만 호출 가능하므로
        threading.Thread로 감싸면 안 됨 (창이 응답없음 상태가 됨).
        request_daily 등은 내부적으로 QEventLoop을 써서 UI를 막지 않고 처리한다.
        """
        self.btn_sync.setEnabled(False)
        self.btn_sync.setText("☁ 동기화 중...")
        self.statusBar().showMessage("Supabase 동기화 시작...")
        QApplication.processEvents()

        try:
            from supabase_sync import sync_all

            def _progress(code, name):
                self.statusBar().showMessage(f"동기화 중: [{code}] {name}")
                QApplication.processEvents()

            result = sync_all(self.api, on_progress=_progress)
        except Exception as e:
            result = {"error": str(e)}

        self._on_sync_done(result)

    def _on_sync_done(self, result: dict):
        self.btn_sync.setEnabled(True)
        self.btn_sync.setText("☁  Supabase 동기화")
        if "error" in result:
            self.statusBar().showMessage(f"동기화 실패: {result['error']}")
            QMessageBox.warning(self, "Supabase 동기화 실패",
                                 f"오류: {result['error']}\n\n"
                                 "supabase_config.py 값 확인 및 requests 설치 여부를 확인하세요.")
        else:
            msg = f"동기화 완료: 성공 {result['success']}건, 실패 {result['failed']}건"
            self.statusBar().showMessage(msg)

    # ════════════════════════════════════════════════════════
    #  Control - 웹사이트에서 종목 하나씩 실시간 조회 요청 처리
    # ════════════════════════════════════════════════════════
    def _query_avg_sync(self, code: str, start: str, end: str, mode: str = "daily",
                        time_start: str = "", time_end: str = "") -> dict:
        """
        live_server가 /api/query-avg 요청받을 때 호출.
        FastAPI(uvicorn) 스레드에서 호출되므로, 실제 키움 API 작업은
        시그널을 통해 메인 스레드로 넘기고 결과가 올 때까지 대기한다.
        time_start/time_end는 분봉(mode="minute") 조회일 때만 사용된다.
        """
        import uuid
        request_id = str(uuid.uuid4())
        ev = threading.Event()
        self._query_events[request_id] = ev
        self.sig_query_request.emit(
            code, start or "", end or "", mode or "daily",
            time_start or "", time_end or "", request_id,
        )

        got_result = ev.wait(timeout=20)
        result = self._query_results.pop(request_id, None)
        self._query_events.pop(request_id, None)

        if not got_result or result is None:
            return {"error": "조회 시간 초과 (PC 프로그램이 바쁘거나 응답이 없습니다)"}
        return result

    def _sync_fundamentals_one(self, code: str, name: str = "") -> dict:
        """
        웹사이트에서 관심종목을 새로 추가한 직후 호출됨.
        pykrx는 64비트 전용이라, main.py(32비트)에서 직접 계산하지 않고
        64비트 파이썬을 subprocess로 실행시켜서 그 종목 하나만 계산·저장한다.
        """
        import subprocess, os

        PYTHON64_CMD = ["py", "-3.14"]   # 평소 64비트 파이썬 실행 명령어와 다르면 여기만 수정

        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            result = subprocess.run(
                PYTHON64_CMD + ["fundamentals_sync_one.py", code, name or code],
                cwd=base_dir,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                err = (result.stderr or result.stdout or "알 수 없는 오류").strip()
                return {"ok": False, "error": err[:300]}
            return {"ok": True}
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "시간 초과 (30초)"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _sync_investor_flow_one(self, code: str) -> dict:
        """
        웹사이트(시장분석 페이지)에서 종목별 수급현황을 검색했을 때 호출됨.
        pykrx는 64비트 전용이라, main.py(32비트)에서 직접 계산하지 않고
        64비트 파이썬을 subprocess로 실행시켜서 그 종목 하나만 계산·저장한다.
        (_sync_fundamentals_one과 동일한 패턴)
        """
        import subprocess, os

        PYTHON64_CMD = ["py", "-3.14"]   # 평소 64비트 파이썬 실행 명령어와 다르면 여기만 수정

        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            result = subprocess.run(
                PYTHON64_CMD + ["investor_flow_fetch.py", code],
                cwd=base_dir,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                err = (result.stderr or result.stdout or "알 수 없는 오류").strip()
                return {"ok": False, "error": err[:300]}
            return {"ok": True}
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "시간 초과 (30초)"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _sync_program_trade_one(self, code: str) -> dict:
        """
        웹사이트(종목분석 페이지)에서 프로그램매매를 조회했을 때 호출됨.
        opt10060은 키움 OCX(32비트, GUI 스레드) 호출이라 subprocess로 못 돌리고,
        시그널로 메인 스레드에 요청을 넘기고 결과를 기다리는 방식으로 처리한다.
        (_search_stock_sync / _on_search_request와 동일한 패턴)
        """
        import uuid
        import datetime as _dt

        request_id = str(uuid.uuid4())
        ev = threading.Event()
        self._program_trade_events[request_id] = ev

        date_to = _dt.date.today().strftime("%Y%m%d")
        date_from = (_dt.date.today() - _dt.timedelta(days=90)).strftime("%Y%m%d")

        self.sig_program_trade_request.emit(code, date_from, date_to, request_id)

        got_result = ev.wait(timeout=30)
        result = self._program_trade_results.pop(request_id, None)
        self._program_trade_events.pop(request_id, None)

        if not got_result or result is None:
            return {"ok": False, "error": "조회 시간 초과"}
        if "error" in result:
            return {"ok": False, "error": result["error"]}

        rows = result.get("data", [])
        if not rows:
            return {"ok": False, "error": "opt10060 응답에 데이터가 없습니다."}

        db_rows = [
            {
                "stock_code": code,
                "trade_date": f"{r['date'][:4]}-{r['date'][4:6]}-{r['date'][6:8]}",
                "buy_amt": r.get("buy_amt", 0),
                "sell_amt": r.get("sell_amt", 0),
                "net_amt": r.get("net_amt", 0),
                "net_chg": r.get("net_chg", 0),
            }
            for r in rows
        ]

        try:
            import requests
            from supabase_config import SUPABASE_URL, SUPABASE_KEY

            headers = {
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
            }

            # 같은 종목의 기존 데이터를 지우고 새로 채움 (investor_flow_fetch.py와 동일한 패턴)
            del_res = requests.delete(
                f"{SUPABASE_URL}/rest/v1/program_trade_flow?stock_code=eq.{code}",
                headers=headers, timeout=10,
            )
            del_res.raise_for_status()

            res = requests.post(
                f"{SUPABASE_URL}/rest/v1/program_trade_flow",
                headers={**headers, "Prefer": "return=minimal"},
                json=db_rows, timeout=15,
            )
            res.raise_for_status()
            return {"ok": True, "synced": len(db_rows)}
        except Exception as e:
            return {"ok": False, "error": f"Supabase 저장 실패: {e}"}

    def _on_program_trade_request(self, code: str, date_from: str, date_to: str, request_id: str):
        """메인 스레드에서 실제 키움 API(opt10060) 호출 (반드시 여기서만 실행)"""
        try:
            result = self.api.request_program_trade(code, date_from, date_to)
            self._program_trade_results[request_id] = result
        except Exception as e:
            self._program_trade_results[request_id] = {"error": str(e)}
        finally:
            ev = self._program_trade_events.get(request_id)
            if ev:
                ev.set()

    def _search_stock_sync(self, query: str) -> dict:
        """live_server가 /api/stock-search 요청받을 때 호출 (키움 API 기반 종목명 검색)"""
        import uuid
        request_id = str(uuid.uuid4())
        ev = threading.Event()
        self._search_events[request_id] = ev
        self.sig_search_request.emit(query, request_id)

        # 최초 호출 시 전체 종목 목록을 만드느라 시간이 좀 걸릴 수 있음
        got_result = ev.wait(timeout=30)
        result = self._search_results.pop(request_id, None)
        self._search_events.pop(request_id, None)

        if not got_result or result is None:
            return {"results": [], "error": "검색 시간 초과"}
        return result

    def _on_search_request(self, query: str, request_id: str):
        """메인 스레드에서 실제 키움 API 호출 (반드시 여기서만 실행)"""
        try:
            results = self.api.search_stock_by_name(query)
            self._search_results[request_id] = {"results": results}
        except Exception as e:
            self._search_results[request_id] = {"results": [], "error": str(e)}
        finally:
            ev = self._search_events.get(request_id)
            if ev:
                ev.set()

    def _on_query_request(self, code: str, start: str, end: str, mode: str,
                          time_start: str, time_end: str, request_id: str):
        """메인 스레드에서 실제 키움 API 호출 (반드시 여기서만 실행)"""
        import datetime as _dt
        try:
            name = self.api.get_stock_name(code) or code
            current_price = self.api.get_current_price(code)

            avg = 0
            candles_out = []

            if mode == "minute":
                # 분봉: start를 조회 날짜로 사용 (비우면 오늘)
                target_date = (start or _dt.date.today().strftime("%Y%m%d")).replace("-", "")
                # 시작/종료 시간: 비우면 09:00 ~ "지금 이 순간의 실제 시각"으로 기본 처리
                ts = (time_start or "0900").replace(":", "")
                te = (time_end or _dt.datetime.now().strftime("%H%M")).replace(":", "")
                result = self.api.request_minute_history(
                    code, target_date, interval="3", time_start=ts, time_end=te
                )
                raw = result.get("candles", [])   # avg_price는 kiwoom_api.py에서 이미 계산됨
                if raw:
                    avg = raw[-1].get("avg_price", 0)
                    candles_out = [
                        {"label": c.get("time", ""), "close": c["close"], "avg_price": c.get("avg_price", 0)}
                        for c in raw
                    ]
            else:
                # 일봉: start~end 기간 세력평단
                if start:
                    end_fmt = (end or _dt.date.today().strftime("%Y%m%d")).replace("-", "")
                    start_fmt = start.replace("-", "")
                    result = self.api.request_daily(code, start_fmt, end_fmt)
                    raw = result.get("candles", [])
                    if raw:
                        avg = raw[-1].get("avg_price", 0)
                        candles_out = [
                            {"label": c.get("date", ""), "close": c["close"], "avg_price": c.get("avg_price", 0)}
                            for c in raw
                        ]

            self._query_results[request_id] = {
                "code": code, "name": name,
                "current_price": current_price, "avg": avg,
                "candles": candles_out,
            }
        except Exception as e:
            self._query_results[request_id] = {"error": str(e)}
        finally:
            ev = self._query_events.get(request_id)
            if ev:
                ev.set()

    # ════════════════════════════════════════════════════════
    #  스캐너 성과검증 - 포착 로그 기록 / 조회
    # ════════════════════════════════════════════════════════
    def _log_scanner_capture(self, code: str, name: str):
        """조건검색 편입 시점을 CSV로 기록 (성과검증용)"""
        import os, csv
        from datetime import datetime
        try:
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_log")
            os.makedirs(log_dir, exist_ok=True)
            today = datetime.now().strftime("%Y%m%d")
            path  = os.path.join(log_dir, f"{today}.csv")
            time_str  = datetime.now().strftime("%H:%M:%S")
            price     = self.api.get_current_price(code)
            is_new    = not os.path.exists(path)
            with open(path, "a", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                if is_new:
                    w.writerow(["time", "code", "name", "price"])
                w.writerow([time_str, code, name, price])
        except Exception as e:
            print(f"[성과검증 로그 오류] {e}")

    def _on_scanner_perf_request(self, date_str: str):
        """성과검증 패널에서 특정 날짜 조회 → 로그 읽고 당일 종가 대비 수익률 계산"""
        import os, csv
        self.statusBar().showMessage(f"성과검증 조회 중... {date_str}")
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_log")
        path = os.path.join(log_dir, f"{date_str}.csv")

        if not os.path.exists(path):
            self.scanner.load_capture_results([])
            self.statusBar().showMessage(f"{date_str} 포착 기록 없음")
            return

        captured = []
        with open(path, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                captured.append(row)

        rows = []
        for c in captured:
            code = c["code"]
            capture_price = int(c.get("price", 0) or 0)
            close_price = capture_price
            try:
                daily = self.api.request_daily(code, date_str, date_str)
                candles = daily.get("candles", [])
                if candles:
                    close_price = candles[-1]["close"]
            except Exception:
                pass
            pct = (close_price - capture_price) / capture_price * 100 if capture_price else 0.0
            rows.append({
                "time": c.get("time", ""), "code": code, "name": c.get("name", ""),
                "capture_price": capture_price, "close_price": close_price, "pct": pct,
            })

        self.scanner.load_capture_results(rows)
        self.statusBar().showMessage(f"성과검증 조회 완료: {date_str} ({len(rows)}종목)")


# ════════════════════════════════════════════════════════════
#  앱 진입점
# ════════════════════════════════════════════════════════════
def main():
    pg.setConfigOptions(antialias=True, foreground="#f0f0f0", background="#1e1e1e")
    app = QApplication(sys.argv)
    app.setFont(QFont("Malgun Gothic", 10))

    api = KiwoomAPI()
    api.setup_events()
    api.login()

    if api.get_login_state() != 1:
        QMessageBox.critical(
            None, "로그인 실패",
            "키움 OpenAPI 로그인에 실패했습니다.\n"
            "영웅문이 실행 중인지 확인하고 다시 시도하세요."
        )
        sys.exit(1)

    win = MainWindow(api)
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
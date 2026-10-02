"use client";

import { useEffect, useState } from "react";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
} from "recharts";
import { supabase, isSupabaseConfigured } from "@/lib/supabase";
import { fetchStockName } from "@/lib/quotes";

interface StockRef {
  code: string;
  name: string;
}

interface SectorRef {
  name: string;
  market: "KOSPI" | "KOSDAQ";
}

interface FlowPoint {
  day: string;
  외국인: number;
  기관: number;
  개인: number;
  연기금: number;
}

type MarketSel = "ALL" | "KOSPI" | "KOSDAQ";

function _rowsToFlowPoints(rows: any[]): FlowPoint[] {
  return rows.map((r) => {
    const d = new Date(r.trade_date);
    return {
      day: `${d.getMonth() + 1}/${d.getDate()}`,
      외국인: Number(r.foreign_amt) || 0,
      기관: Number(r.institution) || 0,
      개인: Number(r.individual) || 0,
      연기금: Number(r.pension) || 0,
    };
  });
}

// stock_investor_flow 테이블은 컬럼명이 다름 (foreign_net / institution_net / ...)
function _stockRowsToFlowPoints(rows: any[]): FlowPoint[] {
  return rows.map((r) => {
    const d = new Date(r.trade_date);
    return {
      day: `${d.getMonth() + 1}/${d.getDate()}`,
      외국인: Number(r.foreign_net) || 0,
      기관: Number(r.institution_net) || 0,
      개인: Number(r.individual_net) || 0,
      연기금: Number(r.pension_net) || 0,
    };
  });
}

// 시장 전체(전체/코스피/코스닥) 수급: market_flow_sync.py(PC 프로그램)가 pykrx로 가져와
// Supabase market_flow_trend 테이블에 저장하고, 이 컴포넌트가 그 값을 읽어옵니다.
function useMarketFlowData(market: MarketSel): { data: FlowPoint[]; loading: boolean } {
  const [data, setData] = useState<FlowPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!isSupabaseConfigured) {
      setLoading(false);
      return;
    }
    let alive = true;
    setLoading(true);
    (async () => {
      const { data: rows } = await supabase
        .from("market_flow_trend")
        .select("*")
        .eq("market", market)
        .order("trade_date", { ascending: false }) // 최신 20일을 가져온 뒤 아래에서 오래된 순으로 뒤집음
        .limit(20);

      if (!alive) return;
      if (rows) setData(_rowsToFlowPoints([...rows].reverse()));
      setLoading(false);
    })();
    return () => {
      alive = false;
    };
  }, [market]);

  return { data, loading };
}

// 업종별 수급: sector_flow_sync.py(PC 프로그램)가 종목별 실제 투자자 데이터를
// sector_stocks 매핑으로 업종 단위 합산해서 Supabase sector_flow_trend에 저장한 값입니다.
function useSectorFlowData(sector?: SectorRef): { data: FlowPoint[]; loading: boolean } {
  const [data, setData] = useState<FlowPoint[]>([]);
  const [loading, setLoading] = useState(!!sector);

  useEffect(() => {
    if (!sector || !isSupabaseConfigured) {
      setLoading(false);
      return;
    }
    let alive = true;
    setLoading(true);
    (async () => {
      const { data: rows } = await supabase
        .from("sector_flow_trend")
        .select("*")
        .eq("market", sector.market)
        .eq("sector_name", sector.name)
        .order("trade_date", { ascending: false }) // 최신 20일을 가져온 뒤 아래에서 오래된 순으로 뒤집음
        .limit(20);

      if (!alive) return;
      if (rows) setData(_rowsToFlowPoints([...rows].reverse()));
      setLoading(false);
    })();
    return () => {
      alive = false;
    };
  }, [sector?.market, sector?.name]);

  return { data, loading };
}

// 개별 종목 수급 (실데이터):
// 1) Supabase stock_investor_flow에 오늘자 캐시가 있는지 먼저 확인
// 2) 없으면 PC 브릿지(ngrok, NEXT_PUBLIC_BRIDGE_URL)에 동기화 요청 → investor_flow_fetch.py 실행 → Supabase 저장
// 3) 완료 후 Supabase 재조회해서 차트 갱신
// PC 프로그램이 꺼져있거나 브릿지에 연결 못 하면, 캐시된 값이라도 있으면 그걸 보여주고
// 없으면 "PC 프로그램 연결 필요" 안내만 표시합니다 (에러로 죽지 않음).
function useStockFlowData(stock?: StockRef): {
  data: FlowPoint[];
  loading: boolean;
  syncing: boolean;
  bridgeError: boolean;
} {
  const [data, setData] = useState<FlowPoint[]>([]);
  const [loading, setLoading] = useState(!!stock);
  const [syncing, setSyncing] = useState(false);
  const [bridgeError, setBridgeError] = useState(false);

  useEffect(() => {
    if (!stock || !isSupabaseConfigured) {
      setLoading(false);
      return;
    }
    let alive = true;
    setLoading(true);
    setBridgeError(false);

    (async () => {
      const query = () =>
        supabase
          .from("stock_investor_flow")
          .select("*")
          .eq("stock_code", stock.code)
          .order("trade_date", { ascending: false }) // 최신 20일을 가져온 뒤 아래에서 오래된 순으로 뒤집음
          .limit(20);

      const { data: rows } = await query();
      if (!alive) return;

      if (rows && rows.length > 0) setData(_stockRowsToFlowPoints([...rows].reverse()));

      const today = new Date().toISOString().slice(0, 10);
      const hasToday = rows?.some((r: any) => r.trade_date === today);

      if (!hasToday) {
        // PC/ngrok과 무관하게 상시 실행되는 클라우드 서비스(Render 등)를 호출
        const bridgeUrl = process.env.NEXT_PUBLIC_INVESTOR_FLOW_API_URL;
        if (bridgeUrl) {
          setSyncing(true);
          try {
            // live_server.py의 다른 엔드포인트들과 동일하게 GET + query param 방식
            // ngrok-skip-browser-warning: liveApi.ts의 다른 브릿지 요청들과 동일하게,
            // ngrok 무료 버전의 경고 페이지를 건너뛰기 위해 필요
            const res = await fetch(
              `${bridgeUrl}/api/sync-investor-flow?code=${encodeURIComponent(stock.code)}`,
              { headers: { "ngrok-skip-browser-warning": "true" } }
            );
            const json = await res.json();
            if (res.ok && json.ok) {
              const { data: freshRows } = await query();
              if (alive && freshRows) setData(_stockRowsToFlowPoints([...freshRows].reverse()));
            } else {
              setBridgeError(true);
            }
          } catch {
            setBridgeError(true); // PC 프로그램/ngrok이 꺼져있는 경우
          }
          if (alive) setSyncing(false);
        } else {
          setBridgeError(true); // 브릿지 URL 자체가 설정 안 된 경우
        }
      }

      if (alive) setLoading(false);
    })();

    return () => {
      alive = false;
    };
  }, [stock?.code]);

  return { data, loading, syncing, bridgeError };
}

export default function FundFlowChart({
  stock,
  sector,
}: {
  stock?: StockRef;
  sector?: SectorRef;
}) {
  const [marketSel, setMarketSel] = useState<MarketSel>("ALL");

  // 위젯 자체 검색창으로 고른 종목 (트리맵에서 넘어온 stock prop과 별개)
  const [searchedStock, setSearchedStock] = useState<StockRef | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchError, setSearchError] = useState<string | null>(null);
  const [searching, setSearching] = useState(false);

  const marketFlow = useMarketFlowData(marketSel);
  const sectorFlow = useSectorFlowData(sector);

  // 검색으로 고른 종목이 있으면 그게 우선, 없으면 트리맵에서 넘어온 stock prop 사용
  const effectiveStock = searchedStock ?? stock;
  const stockFlow = useStockFlowData(effectiveStock);

  // 트리맵에서 새 종목을 클릭하면(stock prop이 바뀌면) 이전 검색 결과를 지워서
  // 트리맵 클릭이 검색 상태에 가려지지 않고 항상 바로 반영되게 함
  useEffect(() => {
    setSearchedStock(null);
    setSearchQuery("");
    setSearchError(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stock?.code]);

  // 우선순위: 업종 선택 > 종목(검색 또는 트리맵) > 시장 전체
  const mode: "sector" | "stock" | "market" = sector ? "sector" : effectiveStock ? "stock" : "market";

  const data = mode === "sector" ? sectorFlow.data : mode === "stock" ? stockFlow.data : marketFlow.data;
  const loading = mode === "sector" ? sectorFlow.loading : mode === "market" ? marketFlow.loading : stockFlow.loading;

  const statusLabel =
    mode === "stock"
      ? stockFlow.syncing
        ? "동기화 중..."
        : stockFlow.bridgeError && data.length === 0
        ? "수급 서비스 연결 실패 (잠시 후 재시도)"
        : loading
        ? "불러오는 중..."
        : "KRX 실데이터"
      : loading
      ? "불러오는 중..."
      : "KRX 실데이터";

  const title =
    mode === "sector"
      ? `📈 ${sector!.name} 업종 수급 현황 (${sector!.market === "KOSPI" ? "코스피" : "코스닥"})`
      : mode === "stock"
      ? `📈 ${effectiveStock!.name} 수급 현황`
      : `📈 수급 현황 (${marketSel === "ALL" ? "시장 전체" : marketSel === "KOSPI" ? "코스피" : "코스닥"})`;

  const handleSearch = async () => {
    const clean = searchQuery.trim();
    if (!clean) return;
    setSearchError(null);
    setSearching(true);

    try {
      if (/^\d{6}$/.test(clean)) {
        const name = await fetchStockName(clean);
        setSearchedStock({ code: clean, name: name || clean });
      } else {
        // 종목코드가 아니라 이름으로 검색한 경우: 업종 트리맵에 이미 있는 종목 매핑 테이블 재사용
        const { data: rows, error } = await supabase
          .from("sector_stocks")
          .select("code, name")
          .ilike("name", `%${clean}%`)
          .limit(1);

        if (error || !rows || rows.length === 0) {
          setSearchError("검색 결과가 없습니다. 종목코드 6자리 또는 정확한 종목명으로 검색해보세요.");
          setSearching(false);
          return;
        }
        setSearchedStock({ code: rows[0].code, name: rows[0].name });
      }
    } catch {
      setSearchError("검색 중 오류가 발생했습니다.");
    }
    setSearching(false);
  };

  const handleReset = () => {
    setSearchedStock(null);
    setSearchQuery("");
    setSearchError(null);
  };

  return (
    <div className="rounded-card border border-border bg-panel p-3">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-[13px] font-bold text-text">{title}</h3>

        <div className="flex flex-wrap items-center gap-2">
          {mode === "market" && (
            <div className="flex overflow-hidden rounded-md border border-border text-[11px]">
              {(["ALL", "KOSPI", "KOSDAQ"] as MarketSel[]).map((m) => (
                <button
                  key={m}
                  onClick={() => setMarketSel(m)}
                  className={`px-2 py-1 font-semibold ${
                    marketSel === m ? "bg-accent text-white" : "text-muted hover:text-text"
                  }`}
                >
                  {m === "ALL" ? "전체" : m === "KOSPI" ? "코스피" : "코스닥"}
                </button>
              ))}
            </div>
          )}

          {!sector && (
            <div className="flex items-center gap-1.5">
              <input
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleSearch()}
                placeholder="종목코드/종목명 검색"
                className="w-36 rounded-md border border-border bg-panel2 px-2 py-1 text-[11px] text-text outline-none focus:border-accent"
              />
              <button
                onClick={handleSearch}
                disabled={searching}
                className="rounded-md bg-accent px-2 py-1 text-[11px] font-semibold text-white hover:opacity-90 disabled:opacity-50"
              >
                조회
              </button>
              {searchedStock && (
                <button
                  onClick={handleReset}
                  className="rounded-md border border-border px-2 py-1 text-[11px] text-muted hover:text-text"
                >
                  전체
                </button>
              )}
            </div>
          )}

          <span className="text-[10px] text-muted">단위: 억원 · {statusLabel}</span>
        </div>
      </div>

      {searchError && <p className="mb-2 text-[11px] text-down">{searchError}</p>}

      <div className="h-64">
        {mode === "stock" && stockFlow.bridgeError && data.length === 0 && !loading ? (
          <div className="flex h-full flex-col items-center justify-center gap-1 text-center text-[12px] text-muted">
            <span>수급 데이터를 가져오지 못했어요.</span>
            <span className="text-[11px]">
              무료 클라우드 서비스가 잠들어 있었을 수 있어요 — 잠시 후 다시 조회해보세요.
            </span>
          </div>
        ) : mode !== "stock" && !loading && data.length === 0 ? (
          <div className="flex h-full items-center justify-center text-center text-[12px] text-muted">
            {mode === "sector"
              ? "아직 이 업종의 수급 데이터가 없습니다. PC 프로그램에서 sector_flow_sync.py를 실행해주세요."
              : "아직 데이터가 없습니다. PC 프로그램에서 market_flow_sync.py를 실행해주세요."}
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 4, right: 12, left: 0, bottom: 0 }}>
              <CartesianGrid stroke="#2c313c" strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="day" tick={{ fill: "#8a8f99", fontSize: 10 }} />
              <YAxis tick={{ fill: "#8a8f99", fontSize: 10 }} width={56} tickFormatter={(v) => v.toLocaleString()} />
              <Tooltip
                contentStyle={{ background: "#1c1f26", border: "1px solid #2c313c", borderRadius: 8, fontSize: 12 }}
                labelFormatter={(label) => `날짜: ${label}`}
                formatter={(v: number) => v.toLocaleString()}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: "#8a8f99" }} />
              <Line type="monotone" dataKey="외국인" stroke="#ff4d4f" strokeWidth={1.6} dot={false} />
              <Line type="monotone" dataKey="기관" stroke="#4d8dff" strokeWidth={1.6} dot={false} />
              <Line type="monotone" dataKey="개인" stroke="#44cc88" strokeWidth={1.6} dot={false} />
              <Line type="monotone" dataKey="연기금" stroke="#8a8f99" strokeWidth={1.4} strokeDasharray="4 3" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}

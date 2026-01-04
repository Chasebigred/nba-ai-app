import { useEffect, useMemo, useRef, useState } from "react";
import Standings from "./Standings";

import logo from "@/assets/nba-ai-logo.png";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";

// Optional (if you installed sonner)
// import { Toaster, toast } from "@/components/ui/sonner";

/**
 * -------------------------
 * Types (API + UI contracts)
 * -------------------------
 * Keeping these close to usage makes it easy to reason about the UI and the backend response shape.
 */
type Leader = {
  player_id: number;
  player_name: string;
  team_abbreviation: string;

  fg3_pct?: number;
  fg3m?: number;
  fg3a?: number;

  fg_pct?: number;
  fgm?: number;
  fga?: number;

  // Used by non-% leaderboards (PPG/RPG/APG/BPG) and sometimes FG%
  value?: number;

  gp: number;
};

type LeadersResponse = {
  season: string;
  min_3pa?: number;
  min_fga?: number;
  min_gp: number;
  limit: number;
  count: number;
  leaders: Leader[];
  generated_at: string;
  source: string;
};

type PlayerHit = {
  nba_player_id: number;
  full_name: string;
  nba_team_id: number | null;
};

type PlayerSearchResponse = {
  query: string;
  count: number;
  players: PlayerHit[];
};

type PlayerLastNGameRow = {
  nba_game_id: string;
  game_date: string | null;
  nba_team_id: number | null;

  minutes: string | null;
  fg_pct: number | null;

  pts: number | null;
  reb: number | null;
  ast: number | null;
  stl: number | null;
  blk: number | null;
  tov: number | null;

  fg3m: number | null;
  fg3a: number | null;

  plus_minus: number | null;
};

type PlayerLastNResponse = {
  nba_player_id: number;
  n: number;
  count: number;
  averages: {
    pts?: number | null;
    reb?: number | null;
    ast?: number | null;
    stl?: number | null;
    blk?: number | null;
    tov?: number | null;
    min?: number | null;

    fg_pct?: number | null;
    fg3_pct?: number | null;
    ft_pct?: number | null;
  };
  games: PlayerLastNGameRow[];
  source: string;
};

/**
 * API base URL:
 * - In dev: defaults to local FastAPI
 * - In prod: set via Vite env var (VITE_API_BASE_URL)
 */
const API = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

/**
 * -------------------------
 * Small helper utilities
 * -------------------------
 * These keep the JSX clean and avoid repeating formatting logic everywhere.
 */
function playerHeadshotUrl(nbaPlayerId: number) {
  return `https://cdn.nba.com/headshots/nba/latest/1040x760/${nbaPlayerId}.png`;
}

function teamLogoUrl(nbaTeamId: number | null | undefined) {
  if (!nbaTeamId) return null;
  return `https://cdn.nba.com/logos/nba/${nbaTeamId}/global/L/logo.svg`;
}

function fmtAvg(x: number | null | undefined, decimals: number = 1) {
  if (x == null) return "—";
  return x.toFixed(decimals);
}

function fmtPct(x: number | null | undefined, decimals: number = 1) {
  if (x == null) return "—";
  return (x * 100).toFixed(decimals) + "%";
}



/**
 * DB game dates sometimes come in as:
 * - "YYYY-MM-DD"
 * - "YYYY-MM-DDTHH:MM:SS..."
 *
 * This keeps display consistent and avoids local timezone shifting by building the date in UTC.
 * (The +1 is to match the existing DB/game date convention in this project.)
 */
function fmtGameDatePlusOne(gameDate: string | null) {
  if (!gameDate) return "—";

  // Accept "YYYY-MM-DD" OR "YYYY-MM-DDTHH:MM:SS..." etc.
  const datePart = gameDate.split("T")[0]; // keep only YYYY-MM-DD
  const [y, m, d] = datePart.split("-").map(Number);

  if (!y || !m || !d) return "—";

  // Build in UTC to avoid local timezone shifting
  const utc = new Date(Date.UTC(y, m - 1, d));
  utc.setUTCDate(utc.getUTCDate() + 1);

  return utc.toLocaleDateString();
}

/**
 * -------------------------
 * Tabs + leaderboard config
 * -------------------------
 */
type AppTab = "home" | "ai" | "player" | "leaders" | "standings";
type LeaderTab = "3pt" | "pts" | "reb" | "ast" | "blk" | "fg";

const LEADER_SUBTABS: Array<{ key: LeaderTab; label: string }> = [
  { key: "3pt", label: "3PT%" },
  { key: "fg", label: "FG%" },
  { key: "pts", label: "PPG" },
  { key: "reb", label: "RPG" },
  { key: "ast", label: "APG" },
  { key: "blk", label: "BPG" },
];

/**
 * Toggle routes on/off during development.
 * If a route isn't wired on the backend yet, the UI can gracefully show a friendly message.
 */
const IMPLEMENTED_ROUTES: Record<LeaderTab, boolean> = {
  "3pt": true,
  "pts": true,
  "reb": true,
  "ast": true,
  "blk": true,
  "fg": true,
};

/**
 * -------------------------
 * Shared UI building blocks
 * -------------------------
 */

/**
 * GlassCard is the "house style" container (dark glass + blur).
 * Using a wrapper makes it easy to keep the design consistent across the app.
 *
 * NOTE: This version passes through any div props (like onClick) so it can be used as a button/card hybrid.
 */
function GlassCard({
  children,
  className = "",
  ...props
}: React.HTMLAttributes<HTMLDivElement> & {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <Card
      {...props}
      className={[
        "bg-slate-950/35 border-slate-800/70 backdrop-blur-xl",
        "shadow-[0_20px_60px_rgba(0,0,0,0.35)]",
        className,
      ].join(" ")}
    >
      {children}
    </Card>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <Card className="bg-slate-950/35 border-slate-800/70 backdrop-blur-xl">
      <CardContent className="p-4">
        <div className="text-xs text-slate-300">{label}</div>
        <div className="mt-1 text-2xl font-semibold tracking-tight text-slate-100">{value}</div>
      </CardContent>
    </Card>
  );
}

/**
 * Small helper header used across pages.
 * Optional right-side content is used for badges/actions.
 */
function SectionHeader({ title, right }: { title: string; right?: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-slate-800/70 bg-slate-950/35 px-4 py-3">
      <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
        <CardTitle className="text-lg text-slate-100 drop-shadow-[0_1px_14px_rgba(59,130,246,0.22)]">
          {title}
        </CardTitle>
        {right}
      </div>
    </div>
  );
}

/**
 * -------------------------
 * Home page (static content)
 * -------------------------
 * This page is intentionally lightweight and explains the app to recruiters / first-time users.
 */
function HomePage() {
  return (
    <div className="grid gap-4">
      <GlassCard>
        <CardHeader className="pb-3">
          <SectionHeader
            title="Project overview"
            right={
              <Badge
                variant="secondary"
                className="bg-slate-950/35 border border-slate-800/70 text-slate-200 w-fit"
              >
                Built by <span className="ml-1 text-slate-100 font-semibold">Chase Brown</span>
              </Badge>
            }
          />
        </CardHeader>

        <CardContent className="pt-0 space-y-4 text-slate-200">
          <p className="text-slate-300 leading-relaxed">
            <span className="font-semibold text-slate-100">NBA Insight</span> is a full-stack NBA analytics application
            focused on fast, clean exploration of player data, league leaders, and standings. The frontend is built with{" "}
            <span className="font-semibold text-slate-100">React + TypeScript</span>, backed by a{" "}
            <span className="font-semibold text-slate-100">FastAPI (Python)</span> service and a{" "}
            <span className="font-semibold text-slate-100">PostgreSQL</span> database optimized for efficient reads.
          </p>

          <div className="grid gap-3 md:grid-cols-2">
            <Card className="bg-slate-950/25 border-slate-800/70">
              <CardHeader className="pb-2">
                <CardTitle className="text-base text-slate-100">What you can do</CardTitle>
              </CardHeader>
              <CardContent className="pt-0 text-sm text-slate-300 space-y-2">
                <ul className="list-disc pl-5 space-y-2">
                  <li>
                    <span className="font-semibold text-slate-100">Search players:</span> Find NBA players stored in the
                    database and view recent game logs along with per-game averages.
                  </li>
                  <li>
                    <span className="font-semibold text-slate-100">Explore leaders:</span> Browse leaderboard categories
                    (3PT%, FG%, PPG, RPG, APG, BPG). Selecting a player jumps directly to their detailed stats page.
                  </li>
                  <li>
                    <span className="font-semibold text-slate-100">Check standings:</span> View current season standings
                    served directly from the database for fast load times.
                  </li>
                </ul>
              </CardContent>
            </Card>

            <Card className="bg-slate-950/25 border-slate-800/70">
              <CardHeader className="pb-2">
                <CardTitle className="text-base text-slate-100">How it works</CardTitle>
              </CardHeader>
              <CardContent className="pt-0 text-sm text-slate-300 space-y-2">
                <ol className="list-decimal pl-5 space-y-2">
                  <li>
                    The React frontend calls{" "}
                    <span className="font-semibold text-slate-100">REST-style JSON endpoints</span> exposed by a{" "}
                    <span className="font-semibold text-slate-100">FastAPI (Python)</span> backend running on AWS.
                  </li>
                  <li>
                    Backend compute queries PostgreSQL to return precomputed player stats, leaderboards, and standings.
                  </li>
                  <li>
                    A scheduled workflow runs nightly (e.g., around{" "}
                    <span className="font-semibold text-slate-100">2 AM</span>) to ingest recent NBA games and{" "}
                    <span className="font-semibold text-slate-100">upsert</span> teams, players, games, and box scores.
                  </li>
                  <li>
                    Standings and leaderboard data are stored in dedicated warehouse tables to ensure consistent, fast
                    reads.
                  </li>
                  <li>
                    The UI consumes only database-backed responses — no live third-party API calls occur during page
                    loads.
                  </li>
                </ol>
              </CardContent>
            </Card>
          </div>
        </CardContent>
      </GlassCard>

      <GlassCard>
        <CardHeader className="pb-3">
          <SectionHeader title="Tech stack & deployment" />
        </CardHeader>

        <CardContent className="pt-0">
          <div className="grid gap-3 md:grid-cols-2">
            <Card className="bg-slate-950/25 border-slate-800/70">
              <CardHeader className="pb-2">
                <CardTitle className="text-base text-slate-100">Tech stack</CardTitle>
              </CardHeader>
              <CardContent className="pt-0 text-sm text-slate-300 space-y-2">
                <ul className="list-disc pl-5 space-y-2">
                  <li>
                    <span className="font-semibold text-slate-100">Frontend:</span> React, TypeScript, Vite, Tailwind CSS,
                    shadcn/ui.
                  </li>
                  <li>
                    <span className="font-semibold text-slate-100">Backend & compute:</span> FastAPI{" "}
                    <span className="font-semibold text-slate-100">(Python)</span>, AWS Lambda, SQLAlchemy ORM.
                  </li>
                  <li>
                    <span className="font-semibold text-slate-100">Database:</span> PostgreSQL with schema versioning via
                    Alembic migrations.
                  </li>
                  <li>
                    <span className="font-semibold text-slate-100">Data ingestion:</span> Scheduled AWS Lambda jobs that
                    keep NBA data current.
                  </li>
                </ul>
              </CardContent>
            </Card>

            <Card className="bg-slate-950/25 border-slate-800/70">
              <CardHeader className="pb-2">
                <CardTitle className="text-base text-slate-100">AWS deployment</CardTitle>
              </CardHeader>
              <CardContent className="pt-0 text-sm text-slate-300 space-y-2">
                <ul className="list-disc pl-5 space-y-2">
                  <li>
                    <span className="font-semibold text-slate-100">Frontend:</span> AWS Amplify (static hosting + CI/CD).
                  </li>
                  <li>
                    <span className="font-semibold text-slate-100">API & compute:</span> FastAPI (Python) running on AWS
                    Lambda.
                  </li>
                  <li>
                    <span className="font-semibold text-slate-100">Database:</span> Amazon RDS (PostgreSQL).
                  </li>
                  <li>
                    <span className="font-semibold text-slate-100">Observability:</span> Amazon CloudWatch logs and
                    metrics.
                  </li>
                </ul>
                <div className="mt-2 text-xs text-slate-400">
                  Production runs on AWS; local development uses Docker-based tooling.
                </div>
              </CardContent>
            </Card>
          </div>
        </CardContent>
      </GlassCard>
    </div>
  );
}


type ChatMsg = {
  id: string;
  role: "user" | "assistant";
  text: string;
};

/**
 * -------------------------
 * AI Page (UI only for now)
 * -------------------------
 * This is a "chat shell" that will later be wired to a real LLM endpoint.
 * For now it provides a clean UX flow: messages, typing state, and example prompts.
 */
function AIPage() {
  const [question, setQuestion] = useState("");
  const [status, setStatus] = useState<"idle" | "thinking">("idle");
  const [messages, setMessages] = useState<ChatMsg[]>([]);

  /**
   * Example prompts are intentionally “strong” so the UI sells the idea of AI-powered analytics.
   * Some are vague on purpose — the future model should be able to clarify and answer anyway.
   */
  const examples = [
    "Compare Stephen Curry and Damian Lillard in the last 5 games.",
    "Summarize Nikola Jokić’s last 10 games (PTS/REB/AST) and FG%.",
    "Which players improved their scoring the most over the last 10 games?",
    "Which star players are trending up? Compare last 10 games vs season averages",
    "Show me the top 5 'winning impact' players by average +/- in the last 3 games.",
    "What is the best single game stat line so far this season? (PTS/REB/AST)",
    "Who has the most minutes? Show me the average and the total for that player.",
    "Who’s been the best all-around player in the last 3 games? (min 25 MPG)",
  ];

  /**
   * Adds a message to the conversation thread.
   * Returns an id so we can later update the placeholder assistant message.
   */
  function addMsg(role: ChatMsg["role"], text: string) {
    const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setMessages((m) => [...m, { id, role, text }]);
    return id;
  }

  /**
   * Temporary placeholder behavior until the AI endpoint is live.
   * This function simulates a request/response flow so the UI feels real.
   */
  function mockAsk() {
    const q = question.trim();
    if (!q) return;

    // 1) Push the user message into the thread immediately
    addMsg("user", q);

    // 2) Clear input + set "thinking" state
    setQuestion("");
    setStatus("thinking");

    // 3) Insert placeholder assistant bubble, then replace it after a short delay
    const assistantId = addMsg("assistant", "…");

    setTimeout(() => {
      setStatus("idle");
      setMessages((m) =>
        m.map((x) =>
          x.id === assistantId
            ? {
                ...x,
                text:
                  "Got it — once AI answers are enabled, I’ll respond here.\n\nFor now, you can use Player / Leaders / Standings to explore the data.",
              }
            : x
        )
      );
    }, 650);
  }

  return (
    <div className="grid gap-4">
      <GlassCard>
        <CardHeader className="pb-3">
          <SectionHeader title="NBAI (NBA AI)" />
        </CardHeader>

        <CardContent className="pt-0 space-y-4">
          <div className="grid gap-3 md:grid-cols-3">
            {/* LEFT: conversation thread + composer (text-message layout) */}
            <div className="md:col-span-2">
              <div className="rounded-2xl border border-slate-800/70 bg-slate-950/25 overflow-hidden flex flex-col h-[620px]">
                <div className="px-4 py-2 bg-slate-950/35 text-sm text-slate-200 font-semibold">Conversation</div>

                {/* Scrollable messages area (matches the Standings ScrollArea scrollbar style) */}
                <ScrollArea className="flex-1">
                  <div className="p-4 space-y-3">
                    {messages.length === 0 ? (
                      <div className="text-sm text-slate-400">Send a message to start the conversation.</div>
                    ) : (
                      messages.map((m) => {
                        const isUser = m.role === "user";
                        return (
                          <div
                            key={m.id}
                            className={["flex items-end gap-2", isUser ? "justify-end" : "justify-start"].join(" ")}
                          >
                            {/* Assistant avatar (logo) */}
                            {!isUser && (
                              <img
                                src={logo}
                                alt="NBAI"
                                className="h-8 w-8 rounded-xl border border-slate-800/70 bg-slate-950/35 object-contain"
                              />
                            )}

                            {/* Message bubble */}
                            <div
                              className={[
                                "max-w-[78%] rounded-2xl px-3 py-2 text-sm leading-relaxed whitespace-pre-wrap",
                                isUser
                                  ? "bg-blue-600/25 border border-blue-500/25 text-slate-100"
                                  : "bg-slate-950/35 border border-slate-800/70 text-slate-200",
                              ].join(" ")}
                            >
                              {m.text}
                            </div>

                            {/* Simple user badge */}
                            {isUser && (
                              <div className="h-8 w-8 rounded-xl border border-slate-800/70 bg-slate-950/35 flex items-center justify-center text-xs text-slate-200">
                                You
                              </div>
                            )}
                          </div>
                        );
                      })
                    )}

                    {/* Typing indicator (assistant) */}
                    {status === "thinking" ? (
                      <div className="flex items-end gap-2 justify-start">
                        <img
                          src={logo}
                          alt="NBAI"
                          className="h-8 w-8 rounded-xl border border-slate-800/70 bg-slate-950/35 object-contain"
                        />
                        <div className="max-w-[78%] rounded-2xl px-3 py-2 text-sm bg-slate-950/35 border border-slate-800/70 text-slate-200">
                          <span className="opacity-80">Typing</span>
                          <span className="inline-block w-6">
                            <span className="animate-pulse">…</span>
                          </span>
                        </div>
                      </div>
                    ) : null}
                  </div>
                </ScrollArea>

                {/* Composer pinned to bottom (like texting) */}
                <div className="border-t border-slate-800/70 bg-slate-950/35 p-3">
                  <textarea
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="Ask NBAI a question…"
                    className="min-h-[70px] w-full resize-none rounded-2xl bg-slate-950/35 border border-slate-800/70 p-3 text-slate-100 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500/40"
                    onKeyDown={(e) => {
                      // Enter to send (Shift+Enter for newline)
                      if (e.key === "Enter" && !e.shiftKey) {
                        e.preventDefault();
                        mockAsk();
                      }
                    }}
                  />

                  <div className="mt-2 flex flex-wrap gap-2 justify-end">
                    <Button
                      onClick={mockAsk}
                      disabled={status === "thinking" || question.trim().length === 0}
                      className="bg-blue-600 hover:bg-blue-500 text-white rounded-xl shadow-[0_0_30px_rgba(37,99,235,0.28)]"
                    >
                      {status === "thinking" ? "Thinking…" : "Send"}
                    </Button>

                    <Button
                      variant="secondary"
                      className="bg-slate-950/35 border border-slate-800/70 text-slate-100 hover:bg-slate-950/55 rounded-xl"
                      onClick={() => {
                        setQuestion("");
                        setMessages([]);
                        setStatus("idle");
                      }}
                    >
                      Clear chat
                    </Button>
                  </div>
                </div>
              </div>
            </div>

            {/* RIGHT: curated example prompts */}
            <Card className="bg-slate-950/25 border-slate-800/70">
              <CardHeader className="pb-2">
                <CardTitle className="text-base text-slate-100">Example prompts</CardTitle>
              </CardHeader>
              <CardContent className="pt-0 text-sm text-slate-300 space-y-2">
                <div className="space-y-2">
                  {examples.map((ex) => (
                    <button
                      key={ex}
                      onClick={() => setQuestion(ex)}
                      className="w-full text-left rounded-xl border border-slate-800/70 bg-slate-950/25 px-3 py-2 hover:bg-slate-950/45 transition"
                    >
                      {ex}
                    </button>
                  ))}
                </div>
              </CardContent>
            </Card>
          </div>
        </CardContent>
      </GlassCard>
    </div>
  );
}

/**
 * -------------------------
 * App (top-level UI + data flow)
 * -------------------------
 * This file intentionally keeps the app in one place for now:
 * - Easy to review on GitHub
 * - Easy to demo to recruiters
 * - Easy to refactor later into /pages and /services once everything is deployed
 */
export default function App() {
  const [tab, setTab] = useState<AppTab>("home");

  // -------------------------
  // Top-level tab transition (rolling ball wipe)
  // -------------------------
  /**
   * Recruiter-friendly rule: only animate "page-level" navigation (home/ai/player/leaders/standings).
   * Sub-tabs (like Leaders category) should stay snappy and instant.
   */
  const [isTabTransitioning, setIsTabTransitioning] = useState(false);
  const pendingTabRef = useRef<AppTab | null>(null);

  // Tweak this if you want it faster/slower.
  const TAB_TRANSITION_MS = 2000;
  // When to actually swap the tab content (under the wipe)
  const TAB_SWAP_MS = 800;
  // Slightly before the ball finishes so it never feels like "extra delay".
  const TAB_UNLOCK_MS = 800;

  function requestTabChange(next: AppTab) {
  if (next === tab) return;
  if (isTabTransitioning) return;

  pendingTabRef.current = next;
  setIsTabTransitioning(true);

  window.setTimeout(() => {
    const target = pendingTabRef.current;
    if (target) setTab(target);
  }, TAB_SWAP_MS);

  window.setTimeout(() => {
    pendingTabRef.current = null;
    setIsTabTransitioning(false);
  }, TAB_UNLOCK_MS);
}

  // -------------------------
  // Leaders state + pagination
  // -------------------------
  const [leaderTab, setLeaderTab] = useState<LeaderTab>("3pt");
  const [leadersStatus, setLeadersStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [leaders, setLeaders] = useState<Leader[]>([]);
  const [leadersLimit, setLeadersLimit] = useState(10);
  const [canLoadMore, setCanLoadMore] = useState(true);

  // -------------------------
  // Player search + selection
  // -------------------------
  const [playerQuery, setPlayerQuery] = useState("");
  const [playerSearchStatus, setPlayerSearchStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [playerResults, setPlayerResults] = useState<PlayerHit[]>([]);
  const [selectedPlayer, setSelectedPlayer] = useState<PlayerHit | null>(null);

  const [playerStatsStatus, setPlayerStatsStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [playerStats, setPlayerStats] = useState<PlayerLastNResponse | null>(null);

  // Default = last 10 games, with an option to show all games.
  const DEFAULT_LAST_N = 10;
  const ALL_GAMES_N = 9999;
  const [showAllGames, setShowAllGames] = useState(false);

  /**
   * Reset paging when the leaderboard sub-tab changes.
   * Example: switching from 3PT% -> PPG should start back at top 10.
   */
  useEffect(() => {
    setLeadersLimit(10);
    setCanLoadMore(true);
  }, [leaderTab]);

  /**
   * Reset paging whenever we re-enter the Leaders page.
   * This keeps the UX predictable when navigating between tabs.
   */
  useEffect(() => {
    if (tab === "leaders") {
      setLeadersLimit(10);
      setCanLoadMore(true);
    }
  }, [tab]);

  /**
   * UI title changes based on the leaderboard category.
   */
  const leadersTitle = useMemo(() => {
    if (leaderTab === "3pt") return "3 Point % Leaders";
    if (leaderTab === "fg") return "Field Goal % Leaders";
    if (leaderTab === "pts") return "Points Per Game Leaders";
    if (leaderTab === "reb") return "Rebounds Per Game Leaders";
    if (leaderTab === "ast") return "Assists Per Game Leaders";
    return "Blocks Per Game Leaders";
  }, [leaderTab]);

  /**
   * Build the leaders endpoint from:
   * - category (leaderTab)
   * - season + minimums
   * - current pagination limit (leadersLimit)
   */
  const leadersEndpoint = useMemo(() => {
    if (leaderTab === "3pt")
      return `${API}/warehouse/leaders/3pt?season=2025-26&min_3pa=50&min_gp=10&limit=${leadersLimit}`;
    if (leaderTab === "pts")
      return `${API}/warehouse/leaders/pts?season=2025-26&min_gp=10&limit=${leadersLimit}`;
    if (leaderTab === "reb")
      return `${API}/warehouse/leaders/reb?season=2025-26&min_gp=10&limit=${leadersLimit}`;
    if (leaderTab === "ast")
      return `${API}/warehouse/leaders/ast?season=2025-26&min_gp=10&limit=${leadersLimit}`;
    if (leaderTab === "blk")
      return `${API}/warehouse/leaders/blk?season=2025-26&min_gp=10&limit=${leadersLimit}`;
    return `${API}/warehouse/leaders/fg?season=2025-26&min_fga=100&min_gp=10&limit=${leadersLimit}`;
  }, [leaderTab, leadersLimit]);

  /**
   * Fetch leaderboard rows from the backend.
   * We keep it as a single list replace (not append) because the API is "limit based".
   */
  async function fetchLeaders() {
    // If a route isn't implemented yet, fail gracefully (useful during development).
    if (!IMPLEMENTED_ROUTES[leaderTab]) {
      setLeaders([]);
      setLeadersStatus("idle");
      setCanLoadMore(true);
      return;
    }

    setLeadersStatus("loading");
    try {
      const r = await fetch(leadersEndpoint);
      if (!r.ok) throw new Error(String(r.status));
      const data: LeadersResponse = await r.json();

      const list = data.leaders ?? [];
      setLeaders(list);

      // If the API returns fewer than requested, we probably hit the end of the leaderboard.
      setCanLoadMore(list.length >= leadersLimit);

      setLeadersStatus("ok");
    } catch {
      setLeadersStatus("error");
    }
  }

  /**
   * Player search is "typeahead" style:
   * - called on a short debounce (below)
   * - returns up to 10 matching players
   */
  function fetchPlayerSearch(q: string) {
    const trimmed = q.trim();
    if (!trimmed) {
      setPlayerResults([]);
      setPlayerSearchStatus("idle");
      return;
    }

    setPlayerSearchStatus("loading");
    fetch(`${API}/warehouse/players/search?q=${encodeURIComponent(trimmed)}&limit=10`)
      .then((r) => r.json())
      .then((data: PlayerSearchResponse) => {
        setPlayerResults(data.players ?? []);
        setPlayerSearchStatus("ok");
      })
      .catch(() => setPlayerSearchStatus("error"));
  }

  /**
   * Fetch recent game logs + averages for a selected player.
   * n controls the window size (last N games vs "all games").
   */
  function fetchPlayerLastN(nbaPlayerId: number, n: number = DEFAULT_LAST_N) {
    setPlayerStatsStatus("loading");
    fetch(`${API}/warehouse/player/${nbaPlayerId}/last_n?n=${n}`)
      .then((r) => r.json())
      .then((data: PlayerLastNResponse) => {
        setPlayerStats(data);
        setPlayerStatsStatus("ok");
      })
      .catch(() => setPlayerStatsStatus("error"));
  }

  /**
   * Leaders -> Player deep link:
   * Clicking a leaderboard row jumps to the Player tab and auto-loads the correct player.
   */
  function jumpToPlayerFromLeader(playerId: number, playerName: string) {
    // Use the animated transition for top-level navigation.
    requestTabChange("player");

    // Clear any previous selection so the player page doesn't briefly show stale data.
    setSelectedPlayer(null);
    setPlayerStats(null);
    setShowAllGames(false);

    // Pre-fill the search so the user sees who we're loading.
    setPlayerQuery(playerName);

    setPlayerSearchStatus("loading");
    fetch(`${API}/warehouse/players/search?q=${encodeURIComponent(playerName)}&limit=5`)
      .then((r) => r.json())
      .then((data: PlayerSearchResponse) => {
        const exact = (data.players ?? []).find((p) => p.nba_player_id === playerId);

        if (exact) {
          setSelectedPlayer(exact);

          // Wipe the search bar + results (matches the UX of clicking a search card).
          setPlayerQuery("");
          setPlayerResults([]);

          fetchPlayerLastN(exact.nba_player_id, DEFAULT_LAST_N);
        }

        setPlayerSearchStatus("ok");
      })
      .catch(() => setPlayerSearchStatus("error"));
  }


  // Initial leaders fetch so the Leaders page has data on first navigation.
  useEffect(() => {
    fetchLeaders();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Re-fetch leaders whenever:
  // - the user changes category (3pt/fg/pts/etc)
  // - pagination changes (Load more)
  // - we are on the Leaders tab (prevents unnecessary requests)
  useEffect(() => {
    if (tab === "leaders") fetchLeaders();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [leaderTab, leadersLimit, tab]);

  /**
   * Debounced player search:
   * As the user types, we wait a moment before hitting the API.
   * This keeps the UI responsive and avoids spamming requests.
   */
  useEffect(() => {
    const t = setTimeout(() => fetchPlayerSearch(playerQuery), 250);
    return () => clearTimeout(t);
  }, [playerQuery]);

  // Shared styling for top-level tabs.
  const triggerBase =
    "rounded-xl px-4 data-[state=active]:bg-blue-600 data-[state=active]:text-white data-[state=active]:shadow-[0_0_24px_rgba(37,99,235,0.25)]";

  return (
    <div className="min-h-screen bg-[radial-gradient(ellipse_at_top,rgba(59,130,246,0.18),transparent_55%),radial-gradient(ellipse_at_bottom,rgba(99,102,241,0.12),transparent_50%)] bg-slate-950 text-slate-100">
      {/* Optional: <Toaster richColors /> */}

      {/* ---------------------------------------------------------
          Tab Transition Overlay (rolling ball wipe)
          - pointer-events prevents interaction while animating
          - overlay is intentionally minimal: fast + clean
         --------------------------------------------------------- */}
      {isTabTransitioning ? (
        <div className="fixed inset-0 z-[999] pointer-events-none overflow-hidden">
          {/* Slight dim so the wipe feels intentional (kept subtle) */}
          <div className="absolute inset-0 bg-slate-950/35 backdrop-blur-[1px]" />

          {/* The "ball" (logo) rolls across the screen */}
          <div
            className="absolute top-1/2 -translate-y-1/2"
            style={{
              // Start off-screen left, end off-screen right.
              animation: `nbaiRollAcross ${TAB_TRANSITION_MS}ms cubic-bezier(0.22, 1, 0.36, 1) forwards`,
              left: "-40vmin",
            }}
          >
            <img
              src={logo}
              alt="Transition"
              draggable={false}
              className="select-none"
              style={{
                width: "42vmin",
                height: "42vmin",
                opacity: 0.8,
                filter: "drop-shadow(0 0 40px rgba(37,99,235,0.35))",
                // Rotation is separate so it feels like a rolling ball.
                animation: `nbaiSpin ${TAB_TRANSITION_MS}ms linear forwards`,
              }}
            />
          </div>

          {/* Keyframes injected once (kept here so you don't need extra CSS files) */}
          <style>
            {`
              @keyframes nbaiRollAcross {
                0%   { transform: translate(-15vw, -50%); }
                100% { transform: translate(120vw, -50%); }
              }
              @keyframes nbaiSpin {
                0%   { transform: rotate(0deg); }
                100% { transform: rotate(720deg); }
              }
              @media (max-width: 640px) {
                /* On small screens, slightly smaller so it doesn't feel overwhelming */
                .nbai-roll img { width: 52vmin; height: 52vmin; }
              }
            `}
          </style>
        </div>
      ) : null}

      <div className="mx-auto max-w-6xl px-5 py-6">
        {/* Top bar (branding + refresh controls) */}
        <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <div className="flex items-center gap-5">
              <img
                src={logo}
                alt="NBA AI"
                className="h-25 w-25 rounded-xl object-contain shadow-[0_0_30px_rgba(37,99,235,0.25)]"
              />

              <div>
                <h1 className="flex items-baseline gap-3 font-display text-3xl md:text-4xl font-bold tracking-[-0.03em]">
                  <span
                    className="
                      text-blue-400
                      drop-shadow-[0_0_32px_rgba(59,130,246,0.55)]
                    "
                  >
                    NBA Insight
                  </span>

                  <span
                    className="
                      text-xs md:text-sm font-semibold
                      px-2.5 py-1 rounded-full
                      border border-blue-400/30
                      bg-blue-500/15
                      text-blue-200
                      shadow-[0_0_16px_rgba(59,130,246,0.35)]
                      relative -top-1
                    "
                  >
                    2025–26
                  </span>
                </h1>

                <p className="text-sm text-slate-300">Player stats • Leaders • Standings</p>
              </div>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
          </div>
        </div>

        <Separator className="my-6 bg-slate-800/80" />

        {/* Main nav (tab-based routing) */}
        <Tabs
          value={tab}
          onValueChange={(v) => {
            // IMPORTANT: Use the animated request here (not setTab directly).
            requestTabChange(v as AppTab);
          }}
        >
          <TabsList className="bg-slate-950/35 border border-slate-800/70 backdrop-blur-xl p-1 rounded-2xl">
            <TabsTrigger value="home" className={triggerBase} disabled={isTabTransitioning}>
              Home
            </TabsTrigger>
            <TabsTrigger value="ai" className={triggerBase} disabled={isTabTransitioning}>
              NBAI
            </TabsTrigger>
            <TabsTrigger value="player" className={triggerBase} disabled={isTabTransitioning}>
              Player
            </TabsTrigger>
            <TabsTrigger value="leaders" className={triggerBase} disabled={isTabTransitioning}>
              Leaders
            </TabsTrigger>
            <TabsTrigger value="standings" className={triggerBase} disabled={isTabTransitioning}>
              Standings
            </TabsTrigger>
          </TabsList>

          <div className="mt-6">
            {/* HOME */}
            {tab === "home" && <HomePage />}

            {/* AI */}
            {tab === "ai" && <AIPage />}

            {/* PLAYER */}
            {tab === "player" && (
              <div className="grid gap-4">
                <GlassCard>
                  <CardHeader className="pb-3">
                    <SectionHeader title="Player Search" />
                  </CardHeader>

                  <CardContent className="pt-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <Input
                        value={playerQuery}
                        onChange={(e) => setPlayerQuery(e.target.value)}
                        placeholder="Search players (e.g., Stephen Curry)"
                        className="max-w-md bg-slate-950/35 border-slate-800/70 text-slate-100 placeholder:text-slate-400"
                      />

                      {selectedPlayer && (
                        <>
                          <Button
                            variant="secondary"
                            className="bg-slate-950/35 border border-slate-800/70 text-slate-100 hover:bg-slate-950/55"
                            onClick={() => {
                              const next = !showAllGames;
                              setShowAllGames(next);
                              fetchPlayerLastN(selectedPlayer.nba_player_id, next ? ALL_GAMES_N : DEFAULT_LAST_N);
                            }}
                          >
                            {showAllGames ? "Show Last 10" : "Show All Games"}
                          </Button>

                          <Button
                            variant="ghost"
                            className="text-slate-300 hover:text-slate-100"
                            onClick={() => {
                              // Reset selection state (keeps the page clean)
                              setSelectedPlayer(null);
                              setPlayerStats(null);
                              setShowAllGames(false);
                              setPlayerQuery("");
                              setPlayerResults([]);
                            }}
                          >
                            Clear
                          </Button>
                        </>
                      )}
                    </div>

                    {/* Search results dropdown */}
                    {playerResults.length > 0 && (
                      <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                        {playerResults.map((p) => (
                          <button
                            key={p.nba_player_id}
                            onClick={() => {
                              // Select player + load stats (default = last 10)
                              setSelectedPlayer(p);
                              setShowAllGames(false);
                              setPlayerQuery("");
                              setPlayerResults([]);
                              fetchPlayerLastN(p.nba_player_id, DEFAULT_LAST_N);
                            }}
                            className="group flex items-center gap-3 rounded-2xl border border-slate-800/70 bg-slate-950/25 p-3 text-left hover:bg-slate-950/45 transition"
                          >
                            <img
                              src={playerHeadshotUrl(p.nba_player_id)}
                              alt={p.full_name}
                              className="h-10 w-14 rounded-xl object-cover bg-slate-900/60 border border-slate-800/70"
                              onError={(e) => {
                                (e.currentTarget as HTMLImageElement).style.display = "none";
                              }}
                            />

                            <div className="min-w-0">
                              <div className="font-semibold text-slate-100 truncate group-hover:text-white">
                                {p.full_name}
                              </div>
                              <div className="text-xs text-slate-300">NBA ID {p.nba_player_id}</div>
                            </div>
                          </button>
                        ))}
                      </div>
                    )}
                  </CardContent>
                </GlassCard>

                {/* Player detail panel (only shows once a player is selected) */}
                {selectedPlayer && (
                  <GlassCard className="overflow-hidden">
                    <CardContent className="p-5">
                      <div className="flex flex-col gap-5 md:flex-row">
                        {/* Left column: headshot + team logo */}
                        <div className="relative shrink-0">
                          <div className="absolute -inset-6 bg-blue-500/10 blur-2xl" />

                          <img
                            src={playerHeadshotUrl(selectedPlayer.nba_player_id)}
                            alt={selectedPlayer.full_name}
                            className="relative w-[220px] rounded-2xl bg-slate-950/35 border border-slate-800/70"
                            onError={(e) => {
                              (e.currentTarget as HTMLImageElement).style.display = "none";
                            }}
                          />

                          {selectedPlayer.nba_team_id ? (
                            <div className="mt-3 flex justify-center">
                              <img
                                src={teamLogoUrl(selectedPlayer.nba_team_id) ?? ""}
                                alt="team"
                                className="h-25 w-25 opacity-95"
                                onError={(e) => {
                                  (e.currentTarget as HTMLImageElement).style.display = "none";
                                }}
                              />
                            </div>
                          ) : null}
                        </div>

                        {/* Right column: name + average stat tiles */}
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
                            <div className="min-w-0">
                              <div className="flex items-center gap-2">
                                <h3 className="text-xl font-semibold tracking-tight truncate text-slate-100">
                                  {selectedPlayer.full_name}
                                </h3>
                                <Badge className="bg-blue-500/15 border border-blue-500/20 text-blue-100">2025–26</Badge>
                              </div>
                            </div>
                          </div>

                          {playerStats?.averages ? (
                            <div className="mt-4">
                              <div className="mb-2 text-sm text-slate-300">
                                <span className="font-semibold text-slate-100">
                                  {showAllGames ? "All games averages" : "Last 10 games averages"}
                                </span>
                              </div>

                              <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
                                <StatCard label="PTS" value={fmtAvg(playerStats.averages.pts, 1)} />
                                <StatCard label="REB" value={fmtAvg(playerStats.averages.reb, 1)} />
                                <StatCard label="AST" value={fmtAvg(playerStats.averages.ast, 1)} />
                                <StatCard label="STL" value={fmtAvg(playerStats.averages.stl, 1)} />
                                <StatCard label="BLK" value={fmtAvg(playerStats.averages.blk, 1)} />
                                <StatCard label="TOV" value={fmtAvg(playerStats.averages.tov, 1)} />
                                <StatCard label="MIN" value={fmtAvg(playerStats.averages.min, 1)} />
                                <StatCard label="FG%" value={fmtPct(playerStats.averages.fg_pct, 1)} />
                                <StatCard label="3P%" value={fmtPct(playerStats.averages.fg3_pct, 1)} />
                              </div>
                            </div>
                          ) : playerStatsStatus === "loading" ? (
                            // Skeletons while loading averages
                            <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
                              {Array.from({ length: 9 }).map((_, i) => (
                                <Skeleton key={i} className="h-[78px] rounded-2xl bg-slate-800/60" />
                              ))}
                            </div>
                          ) : null}
                        </div>
                      </div>

                      {/* Game log table (scrollable) */}
                      <div className="mt-6 overflow-hidden rounded-2xl border border-slate-800/70">
                        <div className="bg-slate-950/35 px-4 py-2 text-sm text-slate-200">
                          Game Log ({playerStats?.games?.length ?? 0})
                        </div>

                        <div className="max-h-[520px] overflow-auto">
                          <Table>
                            <TableHeader className="sticky top-0 bg-slate-950/80 backdrop-blur">
                              <TableRow className="border-slate-800/70">
                                <TableHead className="text-slate-200">Date</TableHead>
                                <TableHead className="text-right text-slate-200">MIN</TableHead>
                                <TableHead className="text-right text-slate-200">PTS</TableHead>
                                <TableHead className="text-right text-slate-200">REB</TableHead>
                                <TableHead className="text-right text-slate-200">AST</TableHead>
                                <TableHead className="text-right text-slate-200">STL</TableHead>
                                <TableHead className="text-right text-slate-200">BLK</TableHead>
                                <TableHead className="text-right text-slate-200">TOV</TableHead>
                                <TableHead className="text-right text-slate-200">FG%</TableHead>
                                <TableHead className="text-right text-slate-200">3PM</TableHead>
                                <TableHead className="text-right text-slate-200">3PA</TableHead>
                                <TableHead className="text-right text-slate-200">+/-</TableHead>
                              </TableRow>
                            </TableHeader>

                            <TableBody>
                              {(playerStats?.games ?? []).map((g) => (
                                <TableRow key={g.nba_game_id} className="hover:bg-slate-950/35 border-slate-800/70">
                                  <TableCell className="text-slate-100">{fmtGameDatePlusOne(g.game_date)}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.minutes ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.pts ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.reb ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.ast ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.stl ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.blk ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.tov ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">
                                    {g.fg_pct != null ? (g.fg_pct * 100).toFixed(1) + "%" : "—"}
                                  </TableCell>
                                  <TableCell className="text-right text-slate-100">{g.fg3m ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.fg3a ?? "—"}</TableCell>
                                  <TableCell className="text-right text-slate-100">{g.plus_minus ?? "—"}</TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                      </div>
                    </CardContent>
                  </GlassCard>
                )}
              </div>
            )}

            {/* LEADERS */}
            {tab === "leaders" && (
              <div className="grid gap-4">
                <GlassCard>
                  <CardHeader className="pb-3">
                    <SectionHeader title={leadersTitle} />
                  </CardHeader>

                  <CardContent className="pt-0">
                    <Tabs value={leaderTab} onValueChange={(v) => setLeaderTab(v as LeaderTab)}>
                      <TabsList className="mt-3 bg-slate-950/35 border border-slate-800/70 backdrop-blur-xl p-1 rounded-2xl w-fit">
                        {LEADER_SUBTABS.map(({ key, label }) => (
                          <TabsTrigger key={key} value={key} className={triggerBase}>
                            {label}
                          </TabsTrigger>
                        ))}
                      </TabsList>
                    </Tabs>

                    {!IMPLEMENTED_ROUTES[leaderTab] ? (
                      <div className="mt-4 rounded-2xl border border-slate-800/70 bg-slate-950/25 p-4 text-slate-200">
                        This leaderboard is not wired on the backend yet:{" "}
                        <code className="text-slate-100">/warehouse/leaders/{leaderTab}</code>
                      </div>
                    ) : null}
                  </CardContent>
                </GlassCard>

                <div className="grid gap-2">
                  {leadersStatus === "loading"
                    ? Array.from({ length: 10 }).map((_, i) => (
                        <Skeleton key={i} className="h-[72px] rounded-2xl bg-slate-800/60" />
                      ))
                    : leaders.map((p, idx) => (
                        <GlassCard
                          key={p.player_id}
                          className="hover:bg-slate-950/45 transition cursor-pointer"
                          role="button"
                          tabIndex={0}
                          onClick={() => jumpToPlayerFromLeader(p.player_id, p.player_name)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") jumpToPlayerFromLeader(p.player_id, p.player_name);
                          }}
                        >
                          <CardContent className="p-3 flex items-center gap-4">
                            <div className="w-10 shrink-0 text-center">
                              <div className="text-xs text-slate-400">RANK</div>
                              <div className="text-2xl font-bold text-slate-100">#{idx + 1}</div>
                            </div>

                            <img
                              src={playerHeadshotUrl(p.player_id)}
                              alt={p.player_name}
                              className="h-12 w-12 rounded-2xl object-cover bg-slate-950/35 border border-slate-800/70"
                              onError={(e) => {
                                (e.currentTarget as HTMLImageElement).style.display = "none";
                              }}
                            />

                            <div className="min-w-0 flex-1">
                              <div className="font-semibold text-blue-200 truncate hover:underline">{p.player_name}</div>
                              <div className="mt-0.5 text-xs text-slate-300 flex items-center gap-2">
                                <span>{p.team_abbreviation}</span>
                                <span className="opacity-60">•</span>
                                <span>GP {p.gp}</span>
                              </div>
                            </div>

                            <div className="text-right">
                              {leaderTab === "3pt" ? (
                                <div className="font-semibold text-blue-200">
                                  {((p.fg3_pct ?? 0) * 100).toFixed(1)}%
                                  <div className="text-xs text-slate-300 font-normal">
                                    {p.fg3m}/{p.fg3a}
                                  </div>
                                </div>
                              ) : leaderTab === "fg" ? (
                                <div className="font-semibold text-blue-200">
                                  {p.value != null ? (p.value * 100).toFixed(1) + "%" : "—"}
                                </div>
                              ) : (
                                <div className="font-semibold text-blue-200">
                                  {p.value != null ? p.value.toFixed(1) : "—"}
                                </div>
                              )}
                            </div>
                          </CardContent>
                        </GlassCard>
                      ))}
                </div>

                {/* Pagination control (limit-based) */}
                {leadersStatus === "ok" ? (
                  <div className="flex justify-center pt-2">
                    <Button
                      variant="secondary"
                      className="bg-slate-950/35 border border-slate-800/70 text-slate-100 hover:bg-slate-950/55 rounded-xl"
                      disabled={!canLoadMore}
                      onClick={() => setLeadersLimit((x) => x + 10)}
                    >
                      {canLoadMore ? "Load more" : "No more players"}
                    </Button>
                  </div>
                ) : null}

                {leadersStatus === "error" ? (
                  <div className="text-rose-300">
                    Failed to load leaders for <code className="text-rose-200">{leaderTab}</code>.
                  </div>
                ) : null}
              </div>
            )}

            {/* STANDINGS */}
            {tab === "standings" && (
              <GlassCard>
                <CardHeader className="pb-3">
                  <SectionHeader title="Standings" />
                </CardHeader>
                <CardContent className="pt-0">
                  <Standings refreshToken={0} />
                </CardContent>
              </GlassCard>
            )}
          </div>
        </Tabs>
      </div>
    </div>
  );
}

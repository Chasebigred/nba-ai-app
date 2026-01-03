import { useEffect, useMemo, useState } from "react";

import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";

/**
 * API base URL:
 * - Local dev defaults to FastAPI on localhost
 * - Production should be set via Vite env var (VITE_API_BASE_URL)
 */
const API = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

/**
 * -------------------------
 * Types (API response shape)
 * -------------------------
 * Matches what the backend returns for the standings endpoint.
 */
type Team = {
  team_id: number | null;
  team_name: string | null;
  team_city?: string | null;
  team_slug?: string | null;
  conference: string | null;
  playoff_rank: number | null;
  wins: number | null;
  losses: number | null;
  win_pct: number | null;
  l10: string | null;
  streak: string | null;
};

type StandingsResponse = {
  season: string;
  generated_at: string | null;
  count: number;
  teams: Team[];
  source: string;
};

/**
 * Sort rules:
 * 1) Conference (so East and West group together)
 * 2) Playoff rank (1..15)
 * 3) Win% (fallback)
 */
function sortStandings(teams: Team[]) {
  return [...teams].sort((a, b) => {
    const ca = (a.conference ?? "").toLowerCase();
    const cb = (b.conference ?? "").toLowerCase();
    if (ca !== cb) return ca.localeCompare(cb);

    const ra = a.playoff_rank ?? 999;
    const rb = b.playoff_rank ?? 999;
    if (ra !== rb) return ra - rb;

    const wa = a.win_pct ?? -1;
    const wb = b.win_pct ?? -1;
    return wb - wa;
  });
}

/**
 * NBA CDN logo URL helper (used for standings team icons).
 */
function teamLogoUrl(teamId: number | null | undefined) {
  if (!teamId) return null;
  return `https://cdn.nba.com/logos/nba/${teamId}/global/L/logo.svg`;
}

/**
 * Formats win% as a percent string.
 * Backend stores win_pct as a decimal (ex: 0.625).
 */
function fmtPct(x: number | null | undefined, decimals: number = 1) {
  if (x == null) return "—";
  return (x * 100).toFixed(decimals) + "%";
}

/**
 * Streak badge (W3 / L2 / etc) with color-coding.
 */
function streakPill(streak: string | null) {
  const s = (streak ?? "").trim();
  if (!s) return <span className="text-slate-300">—</span>;

  const isW = s.toUpperCase().startsWith("W");
  const isL = s.toUpperCase().startsWith("L");

  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium border",
        isW && "bg-emerald-500/10 text-emerald-200 border-emerald-500/20",
        isL && "bg-rose-500/10 text-rose-200 border-rose-500/20",
        !isW && !isL && "bg-slate-900/40 text-slate-200 border-slate-800"
      )}
    >
      {s}
    </span>
  );
}

/**
 * Rank chip with playoffs/play-in styling:
 * - Top 6: Playoffs
 * - 7–10: Play-in
 * - 11–15: Neutral
 */
function rankChip(rank: number | null) {
  const r = rank ?? 999;

  const tone =
    r <= 6
      ? "bg-emerald-500/10 text-emerald-200 border-emerald-500/20"
      : r <= 10
      ? "bg-amber-500/10 text-amber-200 border-amber-500/20"
      : "bg-slate-900/40 text-slate-200 border-slate-800";

  return (
    <span className={cn("inline-flex w-10 justify-center rounded-md border px-2 py-1 text-xs font-semibold", tone)}>
      {rank ?? "—"}
    </span>
  );
}

/**
 * Standings table UI:
 * - Header stays sticky while scrolling
 * - ScrollArea gives consistent scrollbar style across the app (shadcn/ui)
 */
function StandingsTable({ list }: { list: Team[] }) {
  return (
    <div className="overflow-hidden rounded-2xl border border-slate-800 bg-slate-950/20">
      {/* Summary bar */}
      <div className="flex items-center justify-between gap-3 bg-slate-950/45 px-4 py-2">
        <div className="text-sm text-slate-200">
          Teams: <span className="font-semibold text-slate-50">{list.length}</span>
        </div>

        <div className="text-xs text-slate-400">
          Top 6: <span className="text-emerald-200">Playoffs</span> • 7–10: <span className="text-amber-200">Play-in</span>
        </div>
      </div>

      {/* Scrollable table body */}
      <ScrollArea className="h-[560px]">
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-slate-950/95 backdrop-blur">
            <TableRow className="border-slate-800">
              <TableHead className="w-[72px] text-slate-200">Rank</TableHead>
              <TableHead className="text-slate-200">Team</TableHead>
              <TableHead className="text-right text-slate-200">W</TableHead>
              <TableHead className="text-right text-slate-200">L</TableHead>
              <TableHead className="text-right text-slate-200">Win%</TableHead>
              <TableHead className="text-slate-200">L10</TableHead>
              <TableHead className="text-slate-200">Streak</TableHead>
            </TableRow>
          </TableHeader>

          <TableBody>
            {list.map((t, idx) => {
              const logo = teamLogoUrl(t.team_id);
              const name = t.team_name ?? "—";
              const rank = t.playoff_rank ?? null;

              // Light zebra striping for readability
              const zebra = idx % 2 === 1 ? "bg-slate-950/10" : "";

              // Subtle divider at key thresholds: 6 (playoffs) and 10 (play-in)
              const divider =
                rank === 6
                  ? "shadow-[inset_0_-1px_0_0_rgba(16,185,129,0.35)]"
                  : rank === 10
                  ? "shadow-[inset_0_-1px_0_0_rgba(245,158,11,0.35)]"
                  : "";

              return (
                <TableRow
                  key={String(t.team_id ?? name)}
                  className={cn("border-slate-800 hover:bg-slate-950/45 transition", zebra, divider)}
                >
                  <TableCell className="py-2 text-slate-100">{rankChip(rank)}</TableCell>

                  <TableCell className="py-2">
                    <div className="flex items-center gap-3">
                      {logo ? (
                        <div className="grid h-10 w-10 place-items-center rounded-xl bg-slate-900/70 border border-slate-800">
                          <img
                            src={logo}
                            alt={name}
                            className="h-7 w-7"
                            onError={(e) => ((e.currentTarget as HTMLImageElement).style.display = "none")}
                          />
                        </div>
                      ) : null}

                      <div className="min-w-0">
                        <div className="font-semibold text-slate-50 truncate">{name}</div>
                        <div className="text-xs text-slate-400 truncate">{t.team_city ?? ""}</div>
                      </div>
                    </div>
                  </TableCell>

                  <TableCell className="text-right py-2 text-slate-100">{t.wins ?? "—"}</TableCell>
                  <TableCell className="text-right py-2 text-slate-100">{t.losses ?? "—"}</TableCell>
                  <TableCell className="text-right py-2 text-slate-100">{fmtPct(t.win_pct, 1)}</TableCell>
                  <TableCell className="py-2 text-slate-100">{t.l10 ?? "—"}</TableCell>
                  <TableCell className="py-2">{streakPill(t.streak)}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </ScrollArea>
    </div>
  );
}

/**
 * Standings page:
 * - Fetches standings from DB (fast reads)
 * - Supports refresh via refreshToken prop (increment token to re-fetch)
 * - Splits into East/West tabs when possible
 */
export default function Standings({ refreshToken }: { refreshToken: number }) {
  const season = "2025-26";

  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [teams, setTeams] = useState<Team[]>([]);

  /**
   * Pull standings from the backend (DB snapshot).
   */
  function fetchFromDb() {
    setStatus("loading");
    fetch(`${API}/warehouse/standings/current?season=${encodeURIComponent(season)}`)
      .then((r) => r.json())
      .then((data: StandingsResponse) => {
        setTeams(sortStandings(data.teams ?? []));
        setStatus("ok");
      })
      .catch(() => setStatus("error"));
  }

  /**
   * Fetch on mount and whenever refreshToken changes.
   * refreshToken is bumped from the parent when the user hits "Refresh".
   */
  useEffect(() => {
    fetchFromDb();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshToken]);

  /**
   * Derive East/West lists for tab UI.
   * Memo keeps it cheap across re-renders.
   */
  const east = useMemo(() => teams.filter((t) => (t.conference ?? "").toLowerCase().includes("east")), [teams]);
  const west = useMemo(() => teams.filter((t) => (t.conference ?? "").toLowerCase().includes("west")), [teams]);

  // If conference data exists, show tabs. Otherwise fall back to a single full table.
  const hasSplit = east.length > 0 || west.length > 0;

  return (
    <div className="space-y-4">
      {status === "error" ? (
        <div className="rounded-2xl border border-rose-500/25 bg-rose-500/10 p-4 text-rose-200">
          Failed to load standings from DB. Check backend route{" "}
          <code className="text-rose-100">/warehouse/standings/current</code>.
        </div>
      ) : hasSplit ? (
        <Tabs defaultValue="east" className="w-full">
          <TabsList className="bg-slate-950/40 border border-slate-800 p-1">
            <TabsTrigger
              value="east"
              className="
                text-slate-300
                data-[state=active]:bg-blue-600/20
                data-[state=active]:text-blue-200
                data-[state=active]:border
                data-[state=active]:border-blue-500/30
                data-[state=active]:shadow-[0_0_12px_rgba(37,99,235,0.35)]
              "
            >
              East
            </TabsTrigger>

            <TabsTrigger
              value="west"
              className="
                text-slate-300
                data-[state=active]:bg-blue-600/20
                data-[state=active]:text-blue-200
                data-[state=active]:border
                data-[state=active]:border-blue-500/30
                data-[state=active]:shadow-[0_0_12px_rgba(37,99,235,0.35)]
              "
            >
              West
            </TabsTrigger>
          </TabsList>

          <div className="mt-3">
            <TabsContent value="east" className="mt-0">
              <StandingsTable list={east} />
            </TabsContent>
            <TabsContent value="west" className="mt-0">
              <StandingsTable list={west} />
            </TabsContent>
          </div>
        </Tabs>
      ) : (
        <StandingsTable list={teams} />
      )}
    </div>
  );
}

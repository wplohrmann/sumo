import { useEffect, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { api, type RecentBasho } from "../api/client";
import {
  useAllTournaments,
  useCurrentTournament,
  useParticipants,
  useRecentBashos,
} from "../api/hooks";
import PricingTable from "../components/PricingTable";
import PickEntry from "../components/PickEntry";
import AdjustmentsAdmin from "../components/AdjustmentsAdmin";
import AwardsAdmin from "../components/AwardsAdmin";
import TradeAdmin from "../components/TradeAdmin";

const BASHO_NICKNAMES: Record<number, string> = {
  1: "Hatsu",
  3: "Haru",
  5: "Natsu",
  7: "Nagoya",
  9: "Aki",
  11: "Kyushu",
};

function bashoLabel(b: RecentBasho): string {
  const year = b.id.slice(0, 4);
  const month = parseInt(b.id.slice(4, 6), 10);
  const nickname = BASHO_NICKNAMES[month] ?? b.id.slice(4, 6);
  const base = `${nickname} ${year}`;
  if (!b.synced) return `${base} — not synced`;
  if (b.name) return `${base} — ${b.name}`;
  return base;
}

export default function Admin() {
  const t = useCurrentTournament();
  const qc = useQueryClient();
  const participants = useParticipants(t.data?.id);
  const bashos = useRecentBashos();
  const allTournaments = useAllTournaments();

  const [syncBasho, setSyncBasho] = useState("");
  const [tournamentBasho, setTournamentBasho] = useState("");
  const [participantName, setParticipantName] = useState("");
  const [lastToken, setLastToken] = useState<string | null>(null);

  const usedBashoIds = new Set(
    allTournaments.data?.map((x) => x.basho_id) ?? [],
  );

  // Seed dropdown defaults from the recent-bashos list once it loads.
  useEffect(() => {
    if (!bashos.data) return;
    if (!syncBasho && bashos.data.length > 0) {
      setSyncBasho(bashos.data[0].id);
    }
    if (!tournamentBasho) {
      const firstAvailable = bashos.data.find(
        (b) => b.synced && !usedBashoIds.has(b.id),
      );
      if (firstAvailable) setTournamentBasho(firstAvailable.id);
    }
  }, [bashos.data, syncBasho, tournamentBasho, allTournaments.data]);

  const sync = useMutation({
    mutationFn: (basho_id: string) => api.sync(basho_id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["bashos", "recent"] }),
  });

  const createT = useMutation({
    mutationFn: (b: { basho_id: string }) => api.createTournament(b),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tournaments", "current"] }),
  });

  const addP = useMutation({
    mutationFn: ({ tid, name }: { tid: string; name: string }) =>
      api.addParticipant(tid, name),
    onMutate: () => {
      // Drop any previous success token so it doesn't sit there next to an
      // error message from a later attempt.
      setLastToken(null);
    },
    onSuccess: (data) => {
      setLastToken(data.token);
      setParticipantName("");
      qc.invalidateQueries({
        queryKey: ["tournaments", data ? t.data?.id : undefined, "participants"],
      });
    },
  });

  const removeP = useMutation({
    mutationFn: ({ tid, userId }: { tid: string; userId: string }) =>
      api.removeParticipant(tid, userId),
    onSuccess: () => {
      qc.invalidateQueries({
        queryKey: ["tournaments", t.data?.id, "participants"],
      });
      qc.invalidateQueries({
        queryKey: ["tournaments", t.data?.id, "picks"],
      });
    },
  });

  const updateStatus = useMutation({
    mutationFn: (status: "setup" | "drafting" | "active" | "archived") =>
      api.setTournamentStatus(t.data!.id, status),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tournaments", "current"] });
      qc.invalidateQueries({ queryKey: ["tournaments", "all"] });
    },
  });

  const unarchive = useMutation({
    mutationFn: (id: string) => api.setTournamentStatus(id, "active"),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tournaments", "current"] });
      qc.invalidateQueries({ queryKey: ["tournaments", "all"] });
    },
  });

  return (
    <div className="space-y-8">
      <section>
        <h2 className="text-xl font-semibold mb-2">Sync from sumo-api</h2>
        <p className="text-sm text-stone-600 mb-2">
          Pulls Makuuchi banzuke + matches for the selected basho. Idempotent;
          safe to run multiple times.
        </p>
        <form
          className="flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (syncBasho) sync.mutate(syncBasho);
          }}
        >
          <select
            className="flex-1 rounded border border-stone-300 px-3 py-2 bg-white"
            value={syncBasho}
            onChange={(e) => setSyncBasho(e.target.value)}
            disabled={!bashos.data || bashos.data.length === 0}
          >
            {!bashos.data && <option value="">Loading…</option>}
            {bashos.data?.map((b) => (
              <option key={b.id} value={b.id}>
                {bashoLabel(b)}
              </option>
            ))}
          </select>
          <button
            type="submit"
            disabled={sync.isPending || !syncBasho}
            className="px-4 py-2 bg-stone-800 text-white rounded disabled:opacity-50"
          >
            {sync.isPending ? "Syncing…" : "Sync"}
          </button>
        </form>
        {sync.data && (
          <p className="mt-2 text-sm text-green-700">
            Synced. +{sync.data.new_matches} new matches.
          </p>
        )}
        {sync.isError && (
          <p className="mt-2 text-sm text-red-600">{(sync.error as Error).message}</p>
        )}
      </section>

      {allTournaments.data && allTournaments.data.some((x) => x.status === "archived") && (
        <section>
          <h2 className="text-xl font-semibold mb-2">Archived tournaments</h2>
          <p className="text-sm text-stone-600 mb-2">
            {t.data
              ? "Archive the current tournament first to restore one of these."
              : "Restore an archived tournament to make it current again."}
          </p>
          <ul className="space-y-1">
            {allTournaments.data
              .filter((x) => x.status === "archived")
              .map((x) => (
                <li
                  key={x.id}
                  className="flex items-center justify-between gap-2 border border-stone-200 rounded px-3 py-2"
                >
                  <span>{x.name}</span>
                  <button
                    onClick={() => unarchive.mutate(x.id)}
                    disabled={unarchive.isPending || !!t.data}
                    className="px-3 py-1 rounded border border-stone-300 hover:bg-stone-50 disabled:opacity-50"
                  >
                    Unarchive
                  </button>
                </li>
              ))}
          </ul>
          {unarchive.isError && (
            <p className="mt-2 text-sm text-red-600">
              {(unarchive.error as Error).message}
            </p>
          )}
        </section>
      )}

      {!t.data && (
        <section>
          <h2 className="text-xl font-semibold mb-2">Create tournament</h2>
          <p className="text-sm text-stone-600 mb-2">
            Budget is fixed at £55 per the league rules. The tournament name
            is taken from the basho.
          </p>
          <form
            className="space-y-2"
            onSubmit={(e) => {
              e.preventDefault();
              createT.mutate({ basho_id: tournamentBasho });
            }}
          >
            <select
              className="block w-full rounded border border-stone-300 px-3 py-2 bg-white"
              value={tournamentBasho}
              onChange={(e) => setTournamentBasho(e.target.value)}
              disabled={!bashos.data}
            >
              {!bashos.data && <option value="">Loading…</option>}
              {bashos.data && !bashos.data.some((b) => b.synced) && (
                <option value="">Sync a basho first</option>
              )}
              {bashos.data?.map((b) => {
                const used = usedBashoIds.has(b.id);
                const label = used
                  ? `${bashoLabel(b)} — already used`
                  : bashoLabel(b);
                return (
                  <option
                    key={b.id}
                    value={b.id}
                    disabled={!b.synced || used}
                  >
                    {label}
                  </option>
                );
              })}
            </select>
            <button
              type="submit"
              disabled={createT.isPending || !tournamentBasho}
              className="px-4 py-2 bg-stone-800 text-white rounded disabled:opacity-50"
            >
              Create
            </button>
            {createT.isError && (
              <p className="text-sm text-red-600">
                {(createT.error as Error).message}
              </p>
            )}
          </form>
        </section>
      )}

      {t.data && (
        <>
          <section>
            <h2 className="text-xl font-semibold mb-2">Tournament status</h2>
            <p className="text-sm text-stone-600 mb-2">
              Current status: <strong>{t.data.status}</strong>
            </p>
            <div className="flex gap-2 flex-wrap">
              {(["setup", "drafting", "active", "archived"] as const).map((s) => (
                <button
                  key={s}
                  onClick={() => updateStatus.mutate(s)}
                  disabled={s === t.data!.status || updateStatus.isPending}
                  className="px-3 py-1.5 rounded border border-stone-300 hover:bg-stone-50 disabled:opacity-50"
                >
                  {s}
                </button>
              ))}
            </div>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">Add participant</h2>
            <form
              className="flex gap-2"
              onSubmit={(e) => {
                e.preventDefault();
                addP.mutate({ tid: t.data!.id, name: participantName });
              }}
            >
              <input
                className="flex-1 rounded border border-stone-300 px-3 py-2"
                placeholder="display name"
                value={participantName}
                onChange={(e) => setParticipantName(e.target.value)}
              />
              <button
                type="submit"
                disabled={addP.isPending || !participantName}
                className="px-4 py-2 bg-stone-800 text-white rounded disabled:opacity-50"
              >
                Add
              </button>
            </form>
            {addP.isError && (
              <p className="mt-2 text-sm text-red-600">
                {(addP.error as Error).message}
              </p>
            )}
            {lastToken && (
              <div className="mt-2 p-3 rounded bg-yellow-50 border border-yellow-200 text-sm">
                <p className="font-medium">Token (shown once — copy now):</p>
                <code className="block break-all">{lastToken}</code>
              </div>
            )}
            <ul className="mt-3 divide-y rounded border bg-white">
              {participants.data?.map((p) => (
                <li
                  key={p.user_id}
                  className="px-3 py-2 text-sm flex items-center justify-between"
                >
                  <span>{p.display_name}</span>
                  {(t.data!.status === "setup" ||
                    t.data!.status === "drafting") && (
                    <button
                      type="button"
                      onClick={() => {
                        if (
                          window.confirm(
                            `Remove ${p.display_name}? This deletes their picks and invalidates their token.`,
                          )
                        ) {
                          removeP.mutate({
                            tid: t.data!.id,
                            userId: p.user_id,
                          });
                        }
                      }}
                      disabled={removeP.isPending}
                      className="text-xs text-red-700 hover:underline disabled:opacity-50"
                    >
                      remove
                    </button>
                  )}
                </li>
              ))}
            </ul>
            {removeP.isError && (
              <p className="mt-2 text-sm text-red-600">
                {(removeP.error as Error).message}
              </p>
            )}
          </section>

          {(t.data.status === "setup" || t.data.status === "drafting") && (
            <>
              <section>
                <h2 className="text-xl font-semibold mb-2">Rikishi pricing</h2>
                <p className="text-sm text-stone-600 mb-2">
                  Sync the basho first if the list is empty. Prices are in £ and
                  saved on blur.
                </p>
                <div className="rounded border bg-white p-3 max-h-96 overflow-auto">
                  <PricingTable tid={t.data.id} />
                </div>
              </section>

              <section>
                <h2 className="text-xl font-semibold mb-2">Record a draft pick</h2>
                <PickEntry tid={t.data.id} />
              </section>
            </>
          )}

          {(t.data.status === "drafting" || t.data.status === "active") && (
            <section>
              <h2 className="text-xl font-semibold mb-2">Record a trade</h2>
              <p className="text-sm text-stone-600 mb-2">
                Sells one of the participant's rikishi for half its purchase
                price (rounded down) and buys another at the listed price.
              </p>
              <TradeAdmin tid={t.data.id} />
            </section>
          )}

          {t.data.status !== "archived" && (
            <>
              <section>
                <h2 className="text-xl font-semibold mb-2">Score adjustments</h2>
                <p className="text-sm text-stone-600 mb-2">
                  Manual bonuses/deductions. Leave day blank for a whole-tournament
                  adjustment (only visible at day 15).
                </p>
                <AdjustmentsAdmin tid={t.data.id} />
              </section>

              <section>
                <h2 className="text-xl font-semibold mb-2">Yusho / playoff / sansho</h2>
                <p className="text-sm text-stone-600 mb-2">
                  Enter once day 15 is in the books. Yusho is worth 2 pts; each
                  other award is 1 pt and attributed to whoever owns the rikishi
                  at end of day 15.
                </p>
                <AwardsAdmin tid={t.data.id} />
              </section>
            </>
          )}
        </>
      )}
    </div>
  );
}

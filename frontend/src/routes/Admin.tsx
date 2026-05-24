import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { api } from "../api/client";
import { useCurrentTournament, useParticipants } from "../api/hooks";
import PricingTable from "../components/PricingTable";
import PickEntry from "../components/PickEntry";
import AdjustmentsAdmin from "../components/AdjustmentsAdmin";
import AwardsAdmin from "../components/AwardsAdmin";
import TradeAdmin from "../components/TradeAdmin";

export default function Admin() {
  const t = useCurrentTournament();
  const qc = useQueryClient();
  const participants = useParticipants(t.data?.id);

  const [syncBasho, setSyncBasho] = useState("");
  const [tournamentBasho, setTournamentBasho] = useState("");
  const [tournamentName, setTournamentName] = useState("");
  const [budget, setBudget] = useState(55);
  const [participantName, setParticipantName] = useState("");
  const [lastToken, setLastToken] = useState<string | null>(null);

  const sync = useMutation({
    mutationFn: (basho_id: string) => api.sync(basho_id),
  });

  const createT = useMutation({
    mutationFn: (b: { basho_id: string; name: string; budget_pence: number }) =>
      api.createTournament(b),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tournaments", "current"] }),
  });

  const addP = useMutation({
    mutationFn: ({ tid, name }: { tid: string; name: string }) =>
      api.addParticipant(tid, name),
    onSuccess: (data) => {
      setLastToken(data.token);
      setParticipantName("");
      qc.invalidateQueries({
        queryKey: ["tournaments", data ? t.data?.id : undefined, "participants"],
      });
    },
  });

  const updateStatus = useMutation({
    mutationFn: (status: "setup" | "drafting" | "active" | "archived") =>
      api.setTournamentStatus(t.data!.id, status),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tournaments", "current"] }),
  });

  return (
    <div className="space-y-8">
      <section>
        <h2 className="text-xl font-semibold mb-2">Sync from sumo-api</h2>
        <p className="text-sm text-stone-600 mb-2">
          Pulls Makuuchi banzuke + matches for the given basho id (e.g.{" "}
          <code>202405</code>). Idempotent; safe to run multiple times.
        </p>
        <form
          className="flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (syncBasho) sync.mutate(syncBasho);
          }}
        >
          <input
            className="flex-1 rounded border border-stone-300 px-3 py-2"
            placeholder="basho id (YYYYMM)"
            value={syncBasho}
            onChange={(e) => setSyncBasho(e.target.value)}
          />
          <button
            type="submit"
            disabled={sync.isPending}
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

      {!t.data && (
        <section>
          <h2 className="text-xl font-semibold mb-2">Create tournament</h2>
          <form
            className="space-y-2"
            onSubmit={(e) => {
              e.preventDefault();
              createT.mutate({
                basho_id: tournamentBasho,
                name: tournamentName,
                budget_pence: Math.round(budget * 100),
              });
            }}
          >
            <input
              className="block w-full rounded border border-stone-300 px-3 py-2"
              placeholder="basho id (must be synced first)"
              value={tournamentBasho}
              onChange={(e) => setTournamentBasho(e.target.value)}
            />
            <input
              className="block w-full rounded border border-stone-300 px-3 py-2"
              placeholder="league name"
              value={tournamentName}
              onChange={(e) => setTournamentName(e.target.value)}
            />
            <input
              className="block w-full rounded border border-stone-300 px-3 py-2"
              type="number"
              step="0.5"
              value={budget}
              onChange={(e) => setBudget(Number(e.target.value))}
            />
            <button
              type="submit"
              disabled={createT.isPending}
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
            {lastToken && (
              <div className="mt-2 p-3 rounded bg-yellow-50 border border-yellow-200 text-sm">
                <p className="font-medium">Token (shown once — copy now):</p>
                <code className="block break-all">{lastToken}</code>
              </div>
            )}
            <ul className="mt-3 divide-y rounded border bg-white">
              {participants.data?.map((p) => (
                <li key={p.user_id} className="px-3 py-2 text-sm">
                  {p.display_name}
                </li>
              ))}
            </ul>
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

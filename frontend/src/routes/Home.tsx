import { Link } from "react-router-dom";

import {
  useCurrentTournament,
  useMe,
  useParticipants,
  usePicks,
  useStandings,
} from "../api/hooks";
import { pence } from "../lib/money";

export default function Home() {
  const me = useMe();
  const t = useCurrentTournament();
  const participants = useParticipants(t.data?.id);
  const standings = useStandings(t.data?.id);
  const picks = usePicks(t.data?.id);

  if (t.isLoading) return <p>Loading…</p>;
  if (!t.data) {
    return (
      <div className="space-y-2">
        <h2 className="text-xl font-semibold">No active tournament</h2>
        {me.data?.role === "admin" && (
          <p className="text-stone-600">
            Head to the Admin page to sync a basho and create a tournament.
          </p>
        )}
      </div>
    );
  }

  const myRoster = picks.data?.rosters.find(
    (r) => r.user_id === me.data?.id,
  );

  return (
    <div className="space-y-6">
      <header>
        <h2 className="text-2xl font-semibold">{t.data.name}</h2>
        <p className="text-sm text-stone-600">
          basho {t.data.basho_id} · status {t.data.status} · budget{" "}
          {pence(t.data.budget_pence)} · roster {t.data.roster_size}
        </p>
      </header>

      {standings.data?.length ? (
        <section>
          <div className="flex items-baseline justify-between mb-2">
            <h3 className="font-medium">Leaderboard</h3>
            <Link to="/standings" className="text-sm text-blue-700 hover:underline">
              full standings →
            </Link>
          </div>
          <ol className="rounded border bg-white divide-y">
            {standings.data.slice(0, 5).map((s, i) => (
              <li
                key={s.user_id}
                className="px-3 py-2 flex justify-between text-sm"
              >
                <span>
                  <span className="text-stone-400 mr-2 tabular-nums">{i + 1}</span>
                  {s.display_name}
                </span>
                <span className="tabular-nums font-medium">{s.total_points}</span>
              </li>
            ))}
          </ol>
        </section>
      ) : null}

      {myRoster && (
        <section>
          <h3 className="font-medium mb-2">Your roster</h3>
          <ul className="rounded border bg-white divide-y text-sm">
            {myRoster.entries.map((e) => (
              <li key={e.id} className="px-3 py-2 flex justify-between">
                <span>{e.rikishi_name}</span>
                <span className="tabular-nums text-stone-600">
                  {pence(e.purchase_price_pence)}
                </span>
              </li>
            ))}
          </ul>
          <p className="text-xs text-stone-500 mt-1">
            {pence(myRoster.remaining_pence)} unspent
          </p>
        </section>
      )}

      <section>
        <h3 className="font-medium mb-2">Participants</h3>
        <ul className="text-sm text-stone-700 flex flex-wrap gap-2">
          {participants.data?.map((p) => (
            <li
              key={p.user_id}
              className="px-2 py-1 rounded bg-white border text-xs"
            >
              {p.display_name}
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}

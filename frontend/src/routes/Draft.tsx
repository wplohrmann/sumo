import { useCurrentTournament, usePicks } from "../api/hooks";
import { pence } from "../lib/money";

export default function Draft() {
  const t = useCurrentTournament();
  const picks = usePicks(t.data?.id);

  if (!t.data) return <p className="text-stone-600">No active tournament.</p>;
  if (picks.isLoading) return <p>Loading…</p>;
  if (!picks.data) return null;

  return (
    <div className="space-y-4">
      <header>
        <h2 className="text-xl font-semibold">Draft board</h2>
        <p className="text-sm text-stone-600">
          Budget {pence(picks.data.budget_pence)} · roster of{" "}
          {picks.data.roster_size}
        </p>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {picks.data.rosters.map((r) => (
          <div key={r.user_id} className="rounded border bg-white p-4">
            <div className="flex justify-between items-baseline">
              <h3 className="font-medium">{r.display_name}</h3>
              <span className="text-sm text-stone-500">
                {pence(r.remaining_pence)} left
              </span>
            </div>
            <ul className="mt-2 space-y-1 text-sm">
              {Array.from({ length: picks.data.roster_size }).map((_, i) => {
                const entry = r.entries[i];
                if (!entry) {
                  return (
                    <li key={i} className="text-stone-400 italic">
                      — empty —
                    </li>
                  );
                }
                return (
                  <li key={entry.id} className="flex justify-between">
                    <span>
                      {entry.rikishi_name}
                      {entry.acquired_via === "trade" && (
                        <span className="ml-1 text-xs text-stone-500">
                          (traded in)
                        </span>
                      )}
                    </span>
                    <span className="tabular-nums text-stone-600">
                      {pence(entry.purchase_price_pence)}
                    </span>
                  </li>
                );
              })}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}

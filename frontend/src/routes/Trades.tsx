import { useCurrentTournament, useParticipants, useTrades } from "../api/hooks";
import { pence } from "../lib/money";

export default function Trades() {
  const t = useCurrentTournament();
  const trades = useTrades(t.data?.id);
  const participants = useParticipants(t.data?.id);

  if (!t.data) return <p className="text-stone-600">No active tournament.</p>;
  if (trades.isLoading) return <p>Loading…</p>;

  const nameById = new Map(
    participants.data?.map((p) => [p.user_id, p.display_name]) ?? [],
  );

  if (!trades.data?.length) {
    return <p className="text-stone-500 italic">No trades recorded yet.</p>;
  }

  return (
    <div className="space-y-2">
      <h2 className="text-xl font-semibold">Trades</h2>
      <table className="w-full text-sm bg-white rounded border overflow-hidden">
        <thead className="bg-stone-100 text-stone-600">
          <tr>
            <th className="text-left px-3 py-2">Before day</th>
            <th className="text-left px-3 py-2">Player</th>
            <th className="text-left px-3 py-2">Out</th>
            <th className="text-left px-3 py-2">In</th>
            <th className="text-right px-3 py-2">Refund / cost</th>
            <th className="text-left px-3 py-2">Note</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {trades.data.map((tr) => (
            <tr key={tr.id}>
              <td className="px-3 py-2 tabular-nums">d{tr.effective_before_day}</td>
              <td className="px-3 py-2 font-medium">
                {nameById.get(tr.participant_user_id) ?? "?"}
              </td>
              <td className="px-3 py-2">
                {tr.sold_rikishi_name}{" "}
                <span className="text-stone-400">
                  ({pence(tr.sale_price_pence)} refund)
                </span>
              </td>
              <td className="px-3 py-2">
                {tr.bought_rikishi_name}{" "}
                <span className="text-stone-400">
                  ({pence(tr.purchase_price_pence)})
                </span>
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {pence(tr.sale_price_pence - tr.purchase_price_pence)}
              </td>
              <td className="px-3 py-2 text-stone-500">{tr.note}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

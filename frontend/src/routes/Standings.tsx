import { useState } from "react";
import { Link } from "react-router-dom";

import {
  useCurrentTournament,
  useMe,
  useStandings,
} from "../api/hooks";

export default function Standings() {
  const t = useCurrentTournament();
  const me = useMe();
  const userCap = me.data?.spoiler_day ?? 15;
  const [day, setDay] = useState<number>(userCap);
  const standings = useStandings(t.data?.id, day);

  if (!t.data) return <p className="text-stone-600">No active tournament.</p>;
  if (standings.isLoading) return <p>Loading…</p>;
  if (!standings.data) return null;

  return (
    <div className="space-y-4">
      <header className="flex items-baseline justify-between">
        <h2 className="text-xl font-semibold">Standings</h2>
        <p className="text-sm text-stone-600">
          Through day{" "}
          <input
            type="number"
            min={1}
            max={userCap}
            value={day}
            onChange={(e) => setDay(Number(e.target.value))}
            className="w-14 inline-block ml-1 px-1 py-0.5 border border-stone-300 rounded tabular-nums"
          />
        </p>
      </header>

      <table className="w-full text-sm bg-white rounded border overflow-hidden">
        <thead className="bg-stone-100 text-stone-600">
          <tr>
            <th className="text-left px-3 py-2">#</th>
            <th className="text-left px-3 py-2">Player</th>
            <th className="text-right px-3 py-2">Wins</th>
            <th className="text-right px-3 py-2">Scalps</th>
            <th className="text-right px-3 py-2">Awards</th>
            <th className="text-right px-3 py-2">Adj</th>
            <th className="text-right px-3 py-2 font-semibold">Total</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {standings.data.map((s, i) => (
            <tr key={s.user_id}>
              <td className="px-3 py-2 text-stone-500">{i + 1}</td>
              <td className="px-3 py-2 font-medium">{s.display_name}</td>
              <td className="px-3 py-2 text-right tabular-nums">
                {s.wins_points}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {s.scalp_points}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {s.award_points}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {s.adjustment_points}
              </td>
              <td className="px-3 py-2 text-right tabular-nums font-semibold">
                {s.total_points}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <div>
        <h3 className="font-medium mb-1">Per-day timeline</h3>
        <div className="overflow-x-auto rounded border bg-white">
          <table className="text-sm">
            <thead className="bg-stone-100 text-stone-600">
              <tr>
                <th className="text-left px-3 py-2">Player</th>
                {Array.from({ length: day }).map((_, i) => (
                  <th key={i} className="text-right px-2 py-2">
                    <Link
                      to={`/days/${i + 1}`}
                      className="hover:underline"
                    >
                      d{i + 1}
                    </Link>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y">
              {standings.data.map((s) => (
                <tr key={s.user_id}>
                  <td className="px-3 py-2 whitespace-nowrap font-medium">
                    {s.display_name}
                  </td>
                  {s.by_day.map((d) => (
                    <td
                      key={d.day}
                      className="text-right px-2 py-1 tabular-nums"
                    >
                      {d.wins_points + d.scalp_points + d.adjustment_points || ""}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

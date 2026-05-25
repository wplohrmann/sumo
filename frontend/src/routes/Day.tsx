import { useParams, Link } from "react-router-dom";

import { useCurrentTournament, useDay } from "../api/hooks";

export default function Day() {
  const params = useParams<{ day: string }>();
  const day = Number(params.day);
  const t = useCurrentTournament();
  const matches = useDay(t.data?.id, day);

  if (!t.data) return <p className="text-stone-600">No active tournament.</p>;
  if (matches.isLoading) return <p>Loading…</p>;

  return (
    <div className="space-y-4">
      <header className="flex items-baseline justify-between">
        <h2 className="text-xl font-semibold">Day {day}</h2>
        <div className="flex gap-2 text-sm">
          {day > 1 && (
            <Link to={`/days/${day - 1}`} className="hover:underline">
              ← day {day - 1}
            </Link>
          )}
          {day < 15 && (
            <Link to={`/days/${day + 1}`} className="hover:underline">
              day {day + 1} →
            </Link>
          )}
        </div>
      </header>

      {matches.isError && (
        <p className="text-red-600 text-sm">
          {(matches.error as Error).message}
        </p>
      )}

      <table className="w-full text-sm bg-white rounded border overflow-hidden">
        <thead className="bg-stone-100 text-stone-600">
          <tr>
            <th className="text-left px-3 py-2">East</th>
            <th></th>
            <th className="text-right px-3 py-2">West</th>
            <th className="text-left pl-4 px-3 py-2">Kimarite</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {matches.data?.map((m) => {
            const eastWon = m.winner_id === m.rikishi1_id;
            const westWon = m.winner_id === m.rikishi2_id;
            return (
              <tr key={m.match_id}>
                <td
                  className={`px-3 py-2 ${eastWon ? "font-semibold" : "text-stone-500"}`}
                >
                  {m.rikishi1_name}
                  {m.rikishi1_owners.length > 0 && (
                    <span className="ml-2 text-xs text-blue-700">
                      ({m.rikishi1_owners.map((o) => o.display_name).join(", ")})
                    </span>
                  )}
                </td>
                <td className="text-center text-xs text-stone-400">v</td>
                <td
                  className={`px-3 py-2 text-right ${westWon ? "font-semibold" : "text-stone-500"}`}
                >
                  {m.rikishi2_owners.length > 0 && (
                    <span className="mr-2 text-xs text-blue-700">
                      ({m.rikishi2_owners.map((o) => o.display_name).join(", ")})
                    </span>
                  )}
                  {m.rikishi2_name}
                </td>
                <td className="px-3 py-2 text-stone-500">{m.kimarite}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

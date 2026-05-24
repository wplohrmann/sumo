import { useCurrentTournament, useMe, useParticipants } from "../api/hooks";

export default function Home() {
  const me = useMe();
  const t = useCurrentTournament();
  const participants = useParticipants(t.data?.id);

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

  return (
    <div className="space-y-6">
      <header>
        <h2 className="text-2xl font-semibold">{t.data.name}</h2>
        <p className="text-sm text-stone-600">
          basho {t.data.basho_id} · status {t.data.status} · budget £
          {(t.data.budget_pence / 100).toFixed(2)} · roster {t.data.roster_size}
        </p>
      </header>

      <section>
        <h3 className="font-medium mb-2">Participants</h3>
        {participants.data?.length ? (
          <ul className="divide-y rounded border bg-white">
            {participants.data.map((p) => (
              <li key={p.user_id} className="px-3 py-2 text-sm">
                {p.display_name}
              </li>
            ))}
          </ul>
        ) : (
          <p className="text-sm text-stone-500">No participants yet.</p>
        )}
      </section>
    </div>
  );
}

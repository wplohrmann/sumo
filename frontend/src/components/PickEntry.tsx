import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";

import { api, ApiError } from "../api/client";
import { useParticipants, usePicks, useRikishi } from "../api/hooks";
import { pence } from "../lib/money";

export default function PickEntry({ tid }: { tid: string }) {
  const qc = useQueryClient();
  const participants = useParticipants(tid);
  const rikishi = useRikishi(tid);
  const picks = usePicks(tid);
  const [participant, setParticipant] = useState<string>("");
  const [rikishiId, setRikishiId] = useState<string>("");

  const ownedByParticipant = useMemo(() => {
    const map = new Map<string, Set<number>>();
    picks.data?.rosters.forEach((r) => {
      map.set(r.user_id, new Set(r.entries.map((e) => e.rikishi_id)));
    });
    return map;
  }, [picks.data]);

  const create = useMutation({
    mutationFn: ({ force }: { force: boolean }) =>
      api.createPick(tid, participant, Number(rikishiId), force),
    onSuccess: () => {
      setRikishiId("");
      qc.invalidateQueries({ queryKey: ["tournaments", tid, "picks"] });
    },
  });

  const submitPick = () => {
    if (!participant || !rikishiId) return;
    create.mutate(
      { force: false },
      {
        onError: (err) => {
          if (err instanceof ApiError && err.warnings) {
            const lines = err.warnings.map((w) => `• ${w.message}`).join("\n");
            if (
              window.confirm(
                `Sharing rule warning:\n\n${lines}\n\nProceed anyway?`,
              )
            ) {
              create.mutate({ force: true });
            }
          }
        },
      },
    );
  };

  const deletePick = useMutation({
    mutationFn: (entryId: string) => api.deletePick(tid, entryId),
    onSuccess: () =>
      qc.invalidateQueries({ queryKey: ["tournaments", tid, "picks"] }),
  });

  const selectedPrice = rikishi.data?.find(
    (r) => r.rikishi_id === Number(rikishiId),
  )?.price_pence;

  return (
    <div className="space-y-3">
      <form
        className="grid grid-cols-1 md:grid-cols-[1fr_1fr_auto] gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          submitPick();
        }}
      >
        <select
          className="rounded border border-stone-300 px-3 py-2"
          value={participant}
          onChange={(e) => setParticipant(e.target.value)}
          required
        >
          <option value="">— participant —</option>
          {participants.data?.map((p) => (
            <option key={p.user_id} value={p.user_id}>
              {p.display_name}
            </option>
          ))}
        </select>
        <select
          className="rounded border border-stone-300 px-3 py-2"
          value={rikishiId}
          onChange={(e) => setRikishiId(e.target.value)}
          required
        >
          <option value="">— rikishi —</option>
          {rikishi.data
            ?.filter(
              (r) =>
                r.price_pence != null &&
                !(
                  participant &&
                  ownedByParticipant.get(participant)?.has(r.rikishi_id)
                ),
            )
            .map((r) => (
              <option key={r.rikishi_id} value={r.rikishi_id}>
                {r.name} ({r.rank}) — {pence(r.price_pence!)}
              </option>
            ))}
        </select>
        <button
          type="submit"
          className="px-4 py-2 bg-stone-800 text-white rounded disabled:opacity-50"
          disabled={create.isPending}
        >
          Pick
        </button>
      </form>
      {selectedPrice != null && (
        <p className="text-sm text-stone-500">
          Price: {pence(selectedPrice)}
        </p>
      )}
      {create.isError &&
        !(create.error instanceof ApiError && create.error.warnings) && (
          <p className="text-sm text-red-600">
            {(create.error as Error).message}
          </p>
        )}

      {picks.data?.warnings && picks.data.warnings.length > 0 && (
        <div className="rounded border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900">
          <p className="font-medium mb-1">Sharing rule warnings:</p>
          <ul className="list-disc list-inside">
            {picks.data.warnings.map((w, i) => (
              <li key={i}>{w.message}</li>
            ))}
          </ul>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {picks.data?.rosters.map((r) => (
          <div key={r.user_id} className="rounded border bg-white p-3">
            <div className="flex justify-between items-baseline mb-2">
              <h4 className="font-medium">{r.display_name}</h4>
              <span className="text-xs text-stone-500">
                {pence(r.remaining_pence)} left
              </span>
            </div>
            <ul className="space-y-1 text-sm">
              {r.entries.map((e) => (
                <li
                  key={e.id}
                  className="flex justify-between items-center"
                >
                  <span>
                    {e.rikishi_name}
                    <span className="ml-2 text-stone-500">
                      {pence(e.purchase_price_pence)}
                    </span>
                  </span>
                  {e.acquired_via === "draft" && (
                    <button
                      onClick={() => deletePick.mutate(e.id)}
                      className="text-xs text-red-600 hover:underline"
                    >
                      undo
                    </button>
                  )}
                </li>
              ))}
              {r.entries.length === 0 && (
                <li className="text-stone-400 italic">no picks yet</li>
              )}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}

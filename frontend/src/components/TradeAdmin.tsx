import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";

import { api, ApiError } from "../api/client";
import { useParticipants, usePicks, useRikishi } from "../api/hooks";
import { pence } from "../lib/money";

export default function TradeAdmin({ tid }: { tid: string }) {
  const qc = useQueryClient();
  const participants = useParticipants(tid);
  const picks = usePicks(tid);
  const rikishi = useRikishi(tid);

  const [pid, setPid] = useState<string>("");
  const [sellEntryId, setSellEntryId] = useState<string>("");
  const [buyRikishiId, setBuyRikishiId] = useState<string>("");
  const [day, setDay] = useState<number>(2);
  const [note, setNote] = useState<string>("");

  const sellableEntries = useMemo(() => {
    if (!pid || !picks.data) return [];
    return picks.data.rosters.find((r) => r.user_id === pid)?.entries ?? [];
  }, [pid, picks.data]);

  const ownedByParticipant = useMemo(() => {
    const map = new Map<string, Set<number>>();
    picks.data?.rosters.forEach((r) => {
      map.set(r.user_id, new Set(r.entries.map((e) => e.rikishi_id)));
    });
    return map;
  }, [picks.data]);

  const selectedSell = sellableEntries.find((e) => e.id === sellEntryId);
  const selectedBuy = rikishi.data?.find(
    (r) => r.rikishi_id === Number(buyRikishiId),
  );
  const refund = selectedSell
    ? Math.floor(selectedSell.purchase_price_pence / 2)
    : 0;

  const trade = useMutation({
    mutationFn: ({ force }: { force: boolean }) =>
      api.createTrade(tid, {
        participant_user_id: pid,
        sell_entry_id: sellEntryId,
        buy_rikishi_id: Number(buyRikishiId),
        effective_before_day: day,
        note: note || null,
        force,
      }),
    onSuccess: () => {
      setSellEntryId("");
      setBuyRikishiId("");
      setNote("");
      qc.invalidateQueries({ queryKey: ["tournaments", tid] });
    },
  });

  const submitTrade = () => {
    if (!pid || !sellEntryId || !buyRikishiId) return;
    trade.mutate(
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
              trade.mutate({ force: true });
            }
          }
        },
      },
    );
  };

  return (
    <form
      className="space-y-2"
      onSubmit={(e) => {
        e.preventDefault();
        submitTrade();
      }}
    >
      <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
        <select
          className="rounded border border-stone-300 px-3 py-2"
          value={pid}
          onChange={(e) => {
            setPid(e.target.value);
            setSellEntryId("");
          }}
          required
        >
          <option value="">— participant —</option>
          {participants.data?.map((p) => (
            <option key={p.user_id} value={p.user_id}>
              {p.display_name}
            </option>
          ))}
        </select>
        <input
          type="number"
          min={2}
          max={15}
          value={day}
          onChange={(e) => setDay(Number(e.target.value))}
          className="rounded border border-stone-300 px-3 py-2 tabular-nums"
          placeholder="effective before day"
          required
        />
        <select
          className="rounded border border-stone-300 px-3 py-2"
          value={sellEntryId}
          onChange={(e) => setSellEntryId(e.target.value)}
          required
          disabled={!pid}
        >
          <option value="">— sell which rikishi —</option>
          {sellableEntries.map((e) => (
            <option key={e.id} value={e.id}>
              {e.rikishi_name} ({pence(e.purchase_price_pence)} →{" "}
              {pence(Math.floor(e.purchase_price_pence / 2))} refund)
            </option>
          ))}
        </select>
        <select
          className="rounded border border-stone-300 px-3 py-2"
          value={buyRikishiId}
          onChange={(e) => setBuyRikishiId(e.target.value)}
          required
        >
          <option value="">— buy which rikishi —</option>
          {rikishi.data
            ?.filter(
              (r) =>
                r.price_pence != null &&
                !(pid && ownedByParticipant.get(pid)?.has(r.rikishi_id)),
            )
            .map((r) => (
              <option key={r.rikishi_id} value={r.rikishi_id}>
                {r.name} ({r.rank}) — {pence(r.price_pence!)}
              </option>
            ))}
        </select>
      </div>
      <input
        className="w-full rounded border border-stone-300 px-3 py-2"
        placeholder="note (optional)"
        value={note}
        onChange={(e) => setNote(e.target.value)}
      />
      {selectedSell && selectedBuy?.price_pence != null && (
        <p className="text-sm text-stone-600">
          Net cost: {pence(selectedBuy.price_pence - refund)}{" "}
          (buy {pence(selectedBuy.price_pence)} − refund {pence(refund)})
        </p>
      )}
      <button
        type="submit"
        className="px-4 py-2 bg-stone-800 text-white rounded disabled:opacity-50"
        disabled={trade.isPending}
      >
        Record trade
      </button>
      {trade.isError &&
        !(trade.error instanceof ApiError && trade.error.warnings) && (
          <p className="text-sm text-red-600">
            {(trade.error as Error).message}
          </p>
        )}
    </form>
  );
}

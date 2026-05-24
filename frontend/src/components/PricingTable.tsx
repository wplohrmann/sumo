import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api, type RikishiRow } from "../api/client";
import { useRikishi } from "../api/hooks";

function PriceCell({
  tid,
  row,
}: {
  tid: string;
  row: RikishiRow;
}) {
  const qc = useQueryClient();
  const [val, setVal] = useState<string>(
    row.price_pence != null ? (row.price_pence / 100).toString() : "",
  );
  const mut = useMutation({
    mutationFn: (pence: number) => api.setPrice(tid, row.rikishi_id, pence),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tournaments", tid, "rikishi"] }),
  });

  return (
    <input
      type="number"
      step="0.5"
      min="0"
      value={val}
      onChange={(e) => setVal(e.target.value)}
      onBlur={() => {
        const parsed = Number(val);
        const next = Math.round(parsed * 100);
        if (Number.isFinite(parsed) && next !== row.price_pence) {
          mut.mutate(next);
        }
      }}
      className="w-20 px-2 py-1 border border-stone-300 rounded text-right tabular-nums"
    />
  );
}

export default function PricingTable({ tid }: { tid: string }) {
  const rikishi = useRikishi(tid);
  if (rikishi.isLoading) return <p>Loading rikishi…</p>;
  if (!rikishi.data) return null;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-stone-600">
          <th className="py-1 pr-2">Rank</th>
          <th className="py-1 pr-2">Name</th>
          <th className="py-1">Price (£)</th>
        </tr>
      </thead>
      <tbody className="divide-y">
        {rikishi.data.map((r) => (
          <tr key={r.rikishi_id}>
            <td className="py-1 pr-2 text-stone-500">{r.rank}</td>
            <td className="py-1 pr-2 font-medium">{r.name}</td>
            <td className="py-1">
              <PriceCell tid={tid} row={r} />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { type Award } from "../api/client";
import { api } from "../api/client";
import { useAwards, useRikishi } from "../api/hooks";

type Row = { rikishi_id: number; kind: Award["kind"] };

const KINDS: Award["kind"][] = ["yusho", "playoff", "shukun", "kanto", "gino"];

export default function AwardsAdmin({ tid }: { tid: string }) {
  const qc = useQueryClient();
  const existing = useAwards(tid);
  const rikishi = useRikishi(tid);
  const [rows, setRows] = useState<Row[]>([]);

  useEffect(() => {
    if (existing.data && rows.length === 0) {
      setRows(existing.data.map((a) => ({ rikishi_id: a.rikishi_id, kind: a.kind })));
    }
  }, [existing.data, rows.length]);

  const save = useMutation({
    mutationFn: () => api.replaceAwards(tid, rows),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tournaments", tid] }),
  });

  const nameById = new Map(
    rikishi.data?.map((r) => [r.rikishi_id, r.name ?? `#${r.rikishi_id}`]) ?? [],
  );

  return (
    <div className="space-y-3">
      <table className="w-full text-sm bg-white border rounded">
        <thead className="bg-stone-100 text-stone-600">
          <tr>
            <th className="text-left px-3 py-2">Rikishi</th>
            <th className="text-left px-3 py-2">Award</th>
            <th></th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {rows.map((r, i) => (
            <tr key={i}>
              <td className="px-3 py-2">
                <select
                  className="w-full rounded border border-stone-300 px-2 py-1"
                  value={r.rikishi_id || ""}
                  onChange={(e) => {
                    const next = [...rows];
                    next[i] = {
                      ...next[i],
                      rikishi_id: Number(e.target.value),
                    };
                    setRows(next);
                  }}
                >
                  <option value="">—</option>
                  {rikishi.data?.map((rr) => (
                    <option key={rr.rikishi_id} value={rr.rikishi_id}>
                      {rr.name}
                    </option>
                  ))}
                </select>
              </td>
              <td className="px-3 py-2">
                <select
                  className="rounded border border-stone-300 px-2 py-1"
                  value={r.kind}
                  onChange={(e) => {
                    const next = [...rows];
                    next[i] = { ...next[i], kind: e.target.value as Award["kind"] };
                    setRows(next);
                  }}
                >
                  {KINDS.map((k) => (
                    <option key={k} value={k}>
                      {k}
                    </option>
                  ))}
                </select>
              </td>
              <td className="px-3 py-2 text-right">
                <button
                  className="text-xs text-red-600 hover:underline"
                  onClick={() => setRows(rows.filter((_, j) => j !== i))}
                >
                  remove
                </button>
              </td>
            </tr>
          ))}
          {!rows.length && (
            <tr>
              <td colSpan={3} className="px-3 py-2 text-stone-500 italic">
                No awards yet.
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <div className="flex gap-2">
        <button
          className="px-3 py-1.5 border border-stone-300 rounded hover:bg-stone-50"
          onClick={() =>
            setRows([...rows, { rikishi_id: 0, kind: "yusho" }])
          }
        >
          + add row
        </button>
        <button
          className="px-3 py-1.5 bg-stone-800 text-white rounded disabled:opacity-50"
          onClick={() => save.mutate()}
          disabled={save.isPending || rows.some((r) => !r.rikishi_id)}
        >
          Save
        </button>
        {save.isError && (
          <span className="text-sm text-red-600 self-center">
            {(save.error as Error).message}
          </span>
        )}
      </div>
      {existing.data?.length ? (
        <div className="text-xs text-stone-500">
          Currently stored:{" "}
          {existing.data.map((a) => `${nameById.get(a.rikishi_id) ?? a.rikishi_id} (${a.kind})`).join(", ")}
        </div>
      ) : null}
    </div>
  );
}

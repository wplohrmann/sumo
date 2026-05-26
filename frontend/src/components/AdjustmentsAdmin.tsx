import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import {
  useAdjustments,
  useParticipants,
} from "../api/hooks";

export default function AdjustmentsAdmin({ tid }: { tid: string }) {
  const qc = useQueryClient();
  const participants = useParticipants(tid);
  const adjustments = useAdjustments(tid);
  const [pid, setPid] = useState<string>("");
  const [points, setPoints] = useState<number>(1);
  const [reason, setReason] = useState<string>("");
  const [day, setDay] = useState<string>("");

  const add = useMutation({
    mutationFn: () =>
      api.addAdjustment(tid, {
        participant_user_id: pid,
        points,
        reason,
        day: day === "" ? null : Number(day),
      }),
    onSuccess: () => {
      setReason("");
      qc.invalidateQueries({ queryKey: ["tournaments", tid] });
    },
  });

  const del = useMutation({
    mutationFn: (id: string) => api.deleteAdjustment(tid, id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tournaments", tid] }),
  });

  const nameById = new Map(
    participants.data?.map((p) => [p.user_id, p.display_name]) ?? [],
  );

  return (
    <div className="space-y-3">
      <form
        className="grid grid-cols-1 md:grid-cols-[1fr_auto_auto_2fr_auto] gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          if (pid) add.mutate();
        }}
      >
        <select
          className="rounded border border-stone-300 px-3 py-2"
          value={pid}
          onChange={(e) => setPid(e.target.value)}
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
          value={points}
          onChange={(e) => setPoints(Number(e.target.value))}
          className="w-20 rounded border border-stone-300 px-3 py-2 tabular-nums"
          required
        />
        <input
          type="number"
          min={1}
          max={15}
          value={day}
          onChange={(e) => setDay(e.target.value)}
          placeholder="day"
          className="w-20 rounded border border-stone-300 px-3 py-2 tabular-nums"
        />
        <input
          value={reason}
          onChange={(e) => setReason(e.target.value)}
          placeholder="reason"
          className="rounded border border-stone-300 px-3 py-2"
          required
        />
        <button className="px-4 py-2 bg-stone-800 text-white rounded">
          Add
        </button>
      </form>

      <ul className="rounded border bg-white divide-y text-sm">
        {adjustments.data?.map((a) => (
          <li
            key={a.id}
            className="px-3 py-2 flex justify-between items-center"
          >
            <span>
              <span className="font-medium">
                {nameById.get(a.participant_user_id) ?? "?"}
              </span>
              <span className="ml-2 tabular-nums text-stone-700">
                {a.points >= 0 ? `+${a.points}` : a.points}
              </span>
              {a.day != null && (
                <span className="ml-2 text-xs text-stone-500">d{a.day}</span>
              )}
              <span className="ml-2 text-stone-500">{a.reason}</span>
            </span>
            <button
              className="text-xs text-red-600 hover:underline"
              onClick={() => del.mutate(a.id)}
            >
              delete
            </button>
          </li>
        ))}
        {!adjustments.data?.length && (
          <li className="px-3 py-2 text-stone-500 italic">No adjustments.</li>
        )}
      </ul>
    </div>
  );
}

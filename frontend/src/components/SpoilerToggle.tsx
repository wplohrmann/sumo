import { useMe, useSetSpoilerDay } from "../api/hooks";

export default function SpoilerToggle() {
  const me = useMe();
  const set = useSetSpoilerDay();
  if (!me.data) return null;
  const value = me.data.spoiler_day ?? "";

  return (
    <label className="flex items-center gap-1 text-xs text-stone-600">
      Hide past day
      <input
        type="number"
        min={1}
        max={15}
        value={value}
        placeholder="—"
        onChange={(e) => {
          const v = e.target.value;
          set.mutate(v === "" ? null : Number(v));
        }}
        className="w-12 px-1 py-0.5 border border-stone-300 rounded tabular-nums"
      />
    </label>
  );
}

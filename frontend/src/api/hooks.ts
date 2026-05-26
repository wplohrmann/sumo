import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";

import { api } from "./client";

export function useMe() {
  return useQuery({
    queryKey: ["me"],
    queryFn: () => api.me().catch(() => null),
    staleTime: 60_000,
  });
}

export function useLogin() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (token: string) => api.login(token),
    onSuccess: (me) => qc.setQueryData(["me"], me),
  });
}

export function useLogout() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.logout(),
    onSuccess: () => qc.setQueryData(["me"], null),
  });
}

export function useCurrentTournament() {
  return useQuery({
    queryKey: ["tournaments", "current"],
    queryFn: () => api.currentTournament(),
  });
}

export function useAllTournaments() {
  return useQuery({
    queryKey: ["tournaments", "all"],
    queryFn: () => api.listTournaments(),
  });
}

export function useRecentBashos() {
  return useQuery({
    queryKey: ["bashos", "recent"],
    queryFn: () => api.recentBashos(),
    staleTime: 60_000,
  });
}

export function useParticipants(tid: string | undefined) {
  return useQuery({
    queryKey: ["tournaments", tid, "participants"],
    queryFn: () => api.listParticipants(tid!),
    enabled: !!tid,
  });
}

export function useRikishi(tid: string | undefined) {
  return useQuery({
    queryKey: ["tournaments", tid, "rikishi"],
    queryFn: () => api.listRikishi(tid!),
    enabled: !!tid,
  });
}

export function usePicks(tid: string | undefined) {
  return useQuery({
    queryKey: ["tournaments", tid, "picks"],
    queryFn: () => api.listPicks(tid!),
    enabled: !!tid,
  });
}

export function useStandings(tid: string | undefined, throughDay?: number) {
  return useQuery({
    queryKey: ["tournaments", tid, "standings", throughDay],
    queryFn: () => api.standings(tid!, throughDay),
    enabled: !!tid,
  });
}

export function useDay(tid: string | undefined, day: number) {
  return useQuery({
    queryKey: ["tournaments", tid, "day", day],
    queryFn: () => api.day(tid!, day),
    enabled: !!tid && day >= 1 && day <= 15,
  });
}

export function useAdjustments(tid: string | undefined) {
  return useQuery({
    queryKey: ["tournaments", tid, "adjustments"],
    queryFn: () => api.listAdjustments(tid!),
    enabled: !!tid,
  });
}

export function useAwards(tid: string | undefined) {
  return useQuery({
    queryKey: ["tournaments", tid, "awards"],
    queryFn: () => api.listAwards(tid!),
    enabled: !!tid,
  });
}

export function useTrades(tid: string | undefined) {
  return useQuery({
    queryKey: ["tournaments", tid, "trades"],
    queryFn: () => api.listTrades(tid!),
    enabled: !!tid,
  });
}

export function useSetSpoilerDay() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (day: number | null) => api.setSpoilerDay(day),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me"] });
      qc.invalidateQueries({ queryKey: ["tournaments"] });
    },
  });
}

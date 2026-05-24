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

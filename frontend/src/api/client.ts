export type Role = "admin" | "viewer";

export interface Me {
  id: string;
  display_name: string;
  role: Role;
  spoiler_day: number | null;
}

export interface Tournament {
  id: string;
  basho_id: string;
  name: string;
  status: "setup" | "drafting" | "active" | "archived";
  budget_pence: number;
  roster_size: number;
}

export interface Participant {
  user_id: string;
  display_name: string;
}

export interface ParticipantCreated extends Participant {
  token: string;
}

const BASE = "/api";

async function request<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const resp = await fetch(BASE + path, {
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(init.headers ?? {}),
    },
    ...init,
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${resp.status} ${resp.statusText}: ${text}`);
  }
  if (resp.status === 204) return undefined as T;
  return resp.json() as Promise<T>;
}

export const api = {
  login: (token: string) =>
    request<Me>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ token }),
    }),
  logout: () => request<{ ok: boolean }>("/auth/logout", { method: "POST" }),
  me: () => request<Me>("/auth/me"),
  setSpoilerDay: (day: number | null) =>
    request<{ spoiler_day: number | null }>("/me/spoiler-day", {
      method: "PATCH",
      body: JSON.stringify({ spoiler_day: day }),
    }),

  currentTournament: () =>
    request<Tournament | null>("/tournaments/current"),
  getTournament: (id: string) =>
    request<Tournament>(`/tournaments/${id}`),
  listTournaments: () => request<Tournament[]>("/tournaments"),
  createTournament: (body: {
    basho_id: string;
    name: string;
    budget_pence?: number;
    roster_size?: number;
  }) =>
    request<Tournament>("/tournaments", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  setTournamentStatus: (id: string, status: Tournament["status"]) =>
    request<Tournament>(`/tournaments/${id}/status`, {
      method: "PATCH",
      body: JSON.stringify({ status }),
    }),

  listParticipants: (tid: string) =>
    request<Participant[]>(`/tournaments/${tid}/participants`),
  addParticipant: (tid: string, display_name: string) =>
    request<ParticipantCreated>(`/tournaments/${tid}/participants`, {
      method: "POST",
      body: JSON.stringify({ display_name }),
    }),

  sync: (basho_id: string, days = 15) =>
    request<{ new_matches: number; days_synced: number[] }>(
      `/admin/sync?basho_id=${basho_id}&days=${days}`,
      { method: "POST" },
    ),
};

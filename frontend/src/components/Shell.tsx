import { Link, NavLink, Outlet, useNavigate } from "react-router-dom";

import { useLogout, useMe } from "../api/hooks";
import SpoilerToggle from "./SpoilerToggle";

function navLinkCls({ isActive }: { isActive: boolean }) {
  return [
    "px-3 py-2 rounded text-sm font-medium",
    isActive ? "bg-stone-200 text-stone-900" : "text-stone-700 hover:bg-stone-100",
  ].join(" ");
}

export default function Shell() {
  const me = useMe();
  const logout = useLogout();
  const navigate = useNavigate();

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b bg-white">
        <div className="max-w-5xl mx-auto px-4 py-3 flex items-center justify-between">
          <Link to="/" className="font-semibold">
            Fantasy Sumo
          </Link>
          <nav className="flex items-center gap-1">
            <NavLink to="/" className={navLinkCls} end>
              Home
            </NavLink>
            <NavLink to="/standings" className={navLinkCls}>
              Standings
            </NavLink>
            <NavLink to="/draft" className={navLinkCls}>
              Draft
            </NavLink>
            <NavLink to="/trades" className={navLinkCls}>
              Trades
            </NavLink>
            {me.data?.role === "admin" && (
              <NavLink to="/admin" className={navLinkCls}>
                Admin
              </NavLink>
            )}
          </nav>
          <div className="flex items-center gap-3 text-sm text-stone-600">
            <SpoilerToggle />
            {me.data && <span>{me.data.display_name}</span>}
            <button
              className="px-2 py-1 rounded border border-stone-300 hover:bg-stone-50"
              onClick={() =>
                logout.mutate(undefined, { onSuccess: () => navigate("/login") })
              }
            >
              Sign out
            </button>
          </div>
        </div>
      </header>
      <main className="flex-1 max-w-5xl w-full mx-auto px-4 py-6">
        <Outlet />
      </main>
    </div>
  );
}

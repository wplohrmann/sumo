import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { useLogin } from "../api/hooks";

export default function Login() {
  const [token, setToken] = useState("");
  const login = useLogin();
  const navigate = useNavigate();

  return (
    <div className="min-h-screen flex items-center justify-center bg-stone-100">
      <form
        className="w-full max-w-sm p-8 bg-white rounded-lg shadow-sm space-y-4"
        onSubmit={async (e) => {
          e.preventDefault();
          login.mutate(token, {
            onSuccess: () => navigate("/"),
          });
        }}
      >
        <h1 className="text-2xl font-semibold">Fantasy Sumo</h1>
        <label className="block text-sm font-medium text-stone-700">
          Token or admin password
          <input
            className="mt-1 block w-full rounded border-stone-300 shadow-sm focus:border-stone-500 focus:ring-stone-500 px-3 py-2 border"
            type="password"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            autoFocus
            required
          />
        </label>
        <button
          type="submit"
          className="w-full bg-stone-800 text-white py-2 rounded hover:bg-stone-900 disabled:opacity-50"
          disabled={login.isPending}
        >
          {login.isPending ? "Signing in…" : "Sign in"}
        </button>
        {login.isError && (
          <p className="text-sm text-red-600">{(login.error as Error).message}</p>
        )}
      </form>
    </div>
  );
}

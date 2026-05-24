import { Navigate, Route, Routes } from "react-router-dom";

import { useMe } from "./api/hooks";
import Shell from "./components/Shell";
import Login from "./routes/Login";
import Home from "./routes/Home";
import Admin from "./routes/Admin";
import Draft from "./routes/Draft";
import Standings from "./routes/Standings";
import Day from "./routes/Day";
import Trades from "./routes/Trades";

function Protected({ children }: { children: React.ReactNode }) {
  const me = useMe();
  if (me.isLoading) return <p className="p-8">Loading…</p>;
  if (!me.data) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route
        path="/"
        element={
          <Protected>
            <Shell />
          </Protected>
        }
      >
        <Route index element={<Home />} />
        <Route path="admin" element={<Admin />} />
        <Route path="draft" element={<Draft />} />
        <Route path="standings" element={<Standings />} />
        <Route path="days/:day" element={<Day />} />
        <Route path="trades" element={<Trades />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

// ✅ load Inter first so it’s available before your base styles apply
import "@fontsource/inter/index.css";

import "@fontsource/space-grotesk/index.css";

import "./index.css";
import App from "./App.tsx";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>
);

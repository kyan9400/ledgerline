import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

// In development, /api goes to a dashboard you run (or tunnel to) on port 18480.
export default defineConfig({
  plugins: [vue()],
  server: { proxy: { "/api": "http://127.0.0.1:18480" } },
  build: { chunkSizeWarningLimit: 400 },
});

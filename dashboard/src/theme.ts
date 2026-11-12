// The theme: dark by default, light if the viewer chose it (remembered in this browser only).
import { ref, watch } from "vue";

export type Theme = "dark" | "light";
const KEY = "ledgerline-theme";

function initial(): Theme {
  try {
    const saved = localStorage.getItem(KEY);
    if (saved === "light" || saved === "dark") return saved;
  } catch {
    // storage blocked: the default
  }
  return "dark";
}

export const theme = ref<Theme>(initial());

watch(
  theme,
  (t) => {
    document.documentElement.dataset.theme = t;
    try {
      localStorage.setItem(KEY, t);
    } catch {
      // not remembered, still applied
    }
  },
  { immediate: true },
);

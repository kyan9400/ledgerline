<script setup lang="ts">
// A titled panel. With `table`, it can show its chart's numbers as a table instead (every chart has one).
import { ref } from "vue";
import Icon, { PATHS } from "./Icon.vue";

defineProps<{
  title: string;
  note?: string;
  icon?: keyof typeof PATHS;
  legend?: { name: string; role: string }[];
  table?: boolean;
}>();
const asTable = ref(false);
</script>

<template>
  <section class="card">
    <header>
      <div class="title">
        <span v-if="icon" class="chip"><Icon :name="icon" :size="16" /></span>
        <div>
          <h2>{{ title }}</h2>
          <p v-if="note" class="note">{{ note }}</p>
        </div>
      </div>
      <div class="seg" v-if="table" role="group" :aria-label="`${title}: show as`">
        <button :class="{ on: !asTable }" :aria-pressed="!asTable" @click="asTable = false">Chart</button>
        <button :class="{ on: asTable }" :aria-pressed="asTable" @click="asTable = true">Table</button>
      </div>
    </header>
    <ul v-if="legend && legend.length > 1 && !asTable" class="legend">
      <li v-for="l in legend" :key="l.name"><span class="key" :style="{ background: `var(--${l.role})` }"></span>{{ l.name }}</li>
    </ul>
    <div class="body" :class="{ scroll: asTable }">
      <slot v-if="!asTable" />
      <slot v-else name="table" />
    </div>
  </section>
</template>

<style scoped>
.card {
  background: linear-gradient(180deg, var(--surface-2), var(--surface));
  border: 1px solid var(--ring);
  border-radius: 14px;
  padding: 18px 20px 16px;
  min-width: 0;
  box-shadow: var(--shadow);
  transition: border-color 0.2s;
}
.card:hover { border-color: var(--ring-strong); }
header { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 14px; }
.title { display: flex; gap: 12px; align-items: flex-start; min-width: 0; }
.chip {
  flex: none; display: grid; place-items: center; width: 32px; height: 32px; border-radius: 9px;
  color: var(--accent); background: var(--accent-soft); border: 1px solid var(--ring);
}
h2 { font-size: 14.5px; font-weight: 600; margin: 0; letter-spacing: -0.005em; }
.note { margin: 2px 0 0; color: var(--muted); font-size: 12.5px; }
.seg { flex: none; display: inline-flex; padding: 2px; border-radius: 8px; border: 1px solid var(--ring); background: var(--page); }
.seg button {
  border: 0; background: transparent; color: var(--muted); padding: 3px 10px; border-radius: 6px;
  font-size: 12px; cursor: pointer; transition: color 0.15s, background 0.15s;
}
.seg button:hover { color: var(--ink); }
.seg button.on { background: var(--raise); color: var(--ink); font-weight: 600; }
.legend { display: flex; flex-wrap: wrap; gap: 4px 16px; list-style: none; padding: 0; margin: -4px 0 10px; color: var(--ink-2); font-size: 12.5px; }
.legend li { display: flex; align-items: center; gap: 7px; }
.key { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }
.body.scroll { max-height: 300px; overflow: auto; }
</style>

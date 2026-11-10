<script setup lang="ts">
// A titled panel. With `table`, it can show its chart's numbers as a table instead (every chart has one).
import { ref } from "vue";

defineProps<{ title: string; note?: string; legend?: { name: string; role: string }[]; table?: boolean }>();
const asTable = ref(false);
</script>

<template>
  <section class="card">
    <header>
      <div>
        <h2>{{ title }}</h2>
        <p v-if="note" class="note">{{ note }}</p>
      </div>
      <button v-if="table" class="toggle" :aria-pressed="asTable" @click="asTable = !asTable">
        {{ asTable ? "Chart" : "Table" }}
      </button>
    </header>
    <ul v-if="legend && legend.length > 1 && !asTable" class="legend">
      <li v-for="l in legend" :key="l.name"><span class="key" :style="{ background: `var(--${l.role})` }"></span>{{ l.name }}</li>
    </ul>
    <slot v-if="!asTable" />
    <slot v-else name="table" />
  </section>
</template>

<style scoped>
.card {
  background: var(--surface);
  border: 1px solid var(--ring);
  border-radius: 10px;
  padding: 16px 18px 14px;
  min-width: 0;
}
header { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 10px; }
h2 { font-size: 14px; font-weight: 600; margin: 0; }
.note { margin: 2px 0 0; color: var(--ink-2); font-size: 12.5px; }
.toggle {
  border: 1px solid var(--ring); background: transparent; color: var(--ink-2);
  border-radius: 6px; padding: 2px 10px; font-size: 12px; cursor: pointer;
}
.toggle:hover { color: var(--ink); }
.legend { display: flex; flex-wrap: wrap; gap: 4px 16px; list-style: none; padding: 0; margin: 0 0 8px; color: var(--ink-2); font-size: 12.5px; }
.legend li { display: flex; align-items: center; gap: 6px; }
.key { width: 10px; height: 10px; border-radius: 3px; display: inline-block; }
</style>

<script setup lang="ts">
// One chart: bars (stacked or not) or lines over a category axis. Colours come from the CSS tokens at
// draw time, by role (series-1, series-2, ...) and in fixed order, so a theme change repaints them.
import {
  BarController, BarElement, CategoryScale, Chart, Filler, LinearScale, LineController, LineElement,
  PointElement, Tooltip, type ChartConfiguration, type Plugin,
} from "chart.js";
import { onBeforeUnmount, onMounted, ref, watch } from "vue";

Chart.register(BarController, BarElement, LineController, LineElement, PointElement, CategoryScale, LinearScale, Tooltip, Filler);

export interface Series { name: string; values: number[]; role: string }

const props = defineProps<{
  kind: "bar" | "line";
  labels: string[];
  series: Series[];
  stacked?: boolean;
  horizontal?: boolean;
  format: (v: number) => string;
  label: string; // what the chart shows, for screen readers
  height?: number;
}>();

const canvas = ref<HTMLCanvasElement>();
let chart: Chart | null = null;

const token = (name: string) => getComputedStyle(document.documentElement).getPropertyValue(`--${name}`).trim();

// The crosshair: on a line chart, a hairline at the hovered x.
const crosshair: Plugin = {
  id: "crosshair",
  afterDatasetsDraw(c) {
    const active = c.tooltip?.getActiveElements();
    if (props.kind !== "line" || !active?.length) return;
    const x = active[0].element.x;
    const { top, bottom } = c.chartArea;
    const ctx = c.ctx;
    ctx.save();
    ctx.strokeStyle = token("axis");
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x, top);
    ctx.lineTo(x, bottom);
    ctx.stroke();
    ctx.restore();
  },
};

function config(): ChartConfiguration {
  const surface = token("surface");
  const ink2 = token("ink-2");
  const muted = token("muted");
  const grid = token("grid");
  const axis = token("axis");
  const valueAxis = {
    stacked: !!props.stacked,
    beginAtZero: true,
    border: { display: false },
    grid: { color: grid, lineWidth: 1, drawTicks: false },
    ticks: { color: muted, padding: 8, maxTicksLimit: 5, callback: (v: unknown) => props.format(Number(v)) },
  };
  const categoryAxis = {
    stacked: !!props.stacked,
    border: { color: axis },
    grid: { display: false },
    // A short list of named bars shows every name; a time axis thins its labels.
    ticks: { color: muted, maxRotation: 0, autoSkip: !props.horizontal, autoSkipPadding: 16 },
  };
  return {
    type: props.kind,
    data: {
      labels: props.labels,
      datasets: props.series.map((s) => {
        const color = token(s.role);
        return props.kind === "bar"
          ? {
              label: s.name, data: s.values, backgroundColor: color,
              borderColor: surface, borderWidth: props.stacked ? { top: 2 } : 0,
              borderRadius: 4, borderSkipped: "start", maxBarThickness: 28, categoryPercentage: 0.8, barPercentage: 0.9,
            }
          : {
              label: s.name, data: s.values, borderColor: color, backgroundColor: color, borderWidth: 2,
              pointRadius: 0, pointHoverRadius: 4, pointHoverBorderWidth: 2, pointHoverBorderColor: surface,
              tension: 0.25, fill: false,
            };
      }),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      indexAxis: props.horizontal ? "y" : "x",
      interaction: { mode: "index", intersect: props.kind === "bar" && !props.stacked && !!props.horizontal },
      scales: props.horizontal ? { x: valueAxis, y: categoryAxis } : { x: categoryAxis, y: valueAxis },
      plugins: {
        legend: { display: false }, // the card shows its own legend, with direct labels
        tooltip: {
          backgroundColor: surface, titleColor: token("ink"), bodyColor: ink2, borderColor: token("ring"), borderWidth: 1,
          padding: 10, boxWidth: 10, boxHeight: 2, usePointStyle: false, cornerRadius: 6,
          callbacks: { label: (item) => ` ${item.dataset.label}: ${props.format(Number(item.raw))}` },
        },
      },
    } as ChartConfiguration["options"],
    plugins: [crosshair],
  } as ChartConfiguration;
}

function draw() {
  chart?.destroy();
  if (canvas.value) chart = new Chart(canvas.value, config());
}

const scheme = window.matchMedia("(prefers-color-scheme: dark)");
onMounted(() => {
  draw();
  scheme.addEventListener("change", draw);
});
onBeforeUnmount(() => {
  scheme.removeEventListener("change", draw);
  chart?.destroy();
});
watch(() => [props.labels, props.series], () => {
  if (!chart) return draw();
  chart.data.labels = props.labels;
  props.series.forEach((s, i) => (chart!.data.datasets[i].data = s.values));
  chart.update("none");
}, { deep: true });
</script>

<template>
  <div class="plot" :style="{ height: (height ?? 220) + 'px' }">
    <canvas ref="canvas" role="img" :aria-label="label"></canvas>
  </div>
</template>

<style scoped>
.plot { position: relative; width: 100%; }
</style>

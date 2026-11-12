<script setup lang="ts">
// One chart: bars (stacked or not) or lines over a category axis; `area` fills a line with a fading
// gradient, `spark` draws a bare line for a headline card. Colours come from the CSS tokens at draw time,
// by role (series-1, series-2, ...) and in fixed order, so a theme change repaints them.
import {
  BarController, BarElement, CategoryScale, Chart, Filler, LinearScale, LineController, LineElement,
  PointElement, Tooltip, type ChartConfiguration, type Plugin, type ScriptableContext,
} from "chart.js";
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { theme } from "../theme";

Chart.register(BarController, BarElement, LineController, LineElement, PointElement, CategoryScale, LinearScale, Tooltip, Filler);

export interface Series { name: string; values: number[]; role: string }

const props = defineProps<{
  kind: "bar" | "line";
  labels: string[];
  series: Series[];
  stacked?: boolean;
  horizontal?: boolean;
  area?: boolean;
  spark?: boolean;
  format: (v: number) => string;
  label: string; // what the chart shows, for screen readers
  height?: number;
}>();

const canvas = ref<HTMLCanvasElement>();
let chart: Chart | null = null;

const token = (name: string) => getComputedStyle(document.documentElement).getPropertyValue(`--${name}`).trim();

/** A colour at an opacity, from a #rrggbb token (or the colour as it is). */
function alpha(color: string, a: number): string {
  const m = /^#([0-9a-f]{6})$/i.exec(color);
  if (!m) return color;
  const v = parseInt(m[1], 16);
  return `rgba(${(v >> 16) & 255}, ${(v >> 8) & 255}, ${v & 255}, ${a})`;
}

// The crosshair: on a line chart, a hairline at the hovered x.
const crosshair: Plugin = {
  id: "crosshair",
  afterDatasetsDraw(c) {
    const active = c.tooltip?.getActiveElements();
    if (props.kind !== "line" || props.spark || !active?.length) return;
    const x = active[0].element.x;
    const { top, bottom } = c.chartArea;
    const ctx = c.ctx;
    ctx.save();
    ctx.strokeStyle = token("axis");
    ctx.setLineDash([3, 3]);
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
    display: !props.spark,
    stacked: !!props.stacked,
    beginAtZero: true,
    border: { display: false },
    grid: { color: grid, lineWidth: 1, drawTicks: false },
    ticks: { color: muted, padding: 10, maxTicksLimit: 5, font: { size: 11 }, callback: (v: unknown) => props.format(Number(v)) },
  };
  const categoryAxis = {
    display: !props.spark,
    stacked: !!props.stacked,
    border: { color: axis },
    grid: { display: false },
    // A short list of named bars shows every name; a time axis thins its labels.
    ticks: { color: muted, maxRotation: 0, autoSkip: !props.horizontal, autoSkipPadding: 20, font: { size: 11 } },
  };
  return {
    type: props.kind,
    data: {
      labels: props.labels,
      datasets: props.series.map((s) => {
        const color = token(s.role);
        if (props.kind === "bar") {
          return {
            label: s.name, data: s.values, backgroundColor: color, hoverBackgroundColor: alpha(color, 0.8),
            borderColor: surface, borderWidth: props.stacked ? { top: 1 } : 0,
            borderRadius: 5, borderSkipped: "start", maxBarThickness: 26, categoryPercentage: 0.78, barPercentage: 0.9,
          };
        }
        const fill = props.area || props.spark;
        return {
          label: s.name, data: s.values, borderColor: color, borderWidth: props.spark ? 1.6 : 2,
          pointRadius: 0, pointHoverRadius: props.spark ? 0 : 4, pointHoverBorderWidth: 2, pointHoverBorderColor: surface,
          pointHoverBackgroundColor: color, tension: 0.35, cubicInterpolationMode: "monotone",
          fill: fill ? "origin" : false,
          backgroundColor: fill
            ? (ctx: ScriptableContext<"line">) => {
                const area = ctx.chart.chartArea;
                if (!area) return alpha(color, 0.15);
                const g = ctx.chart.ctx.createLinearGradient(0, area.top, 0, area.bottom);
                g.addColorStop(0, alpha(color, props.spark ? 0.28 : 0.32));
                g.addColorStop(1, alpha(color, 0));
                return g;
              }
            : color,
        };
      }),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: { padding: props.spark ? { top: 2, bottom: 0 } : 0 },
      indexAxis: props.horizontal ? "y" : "x",
      interaction: { mode: "index", intersect: props.kind === "bar" && !props.stacked && !!props.horizontal },
      events: props.spark ? [] : undefined,
      scales: props.horizontal ? { x: valueAxis, y: categoryAxis } : { x: categoryAxis, y: valueAxis },
      plugins: {
        legend: { display: false }, // the card shows its own legend
        tooltip: {
          enabled: !props.spark,
          backgroundColor: token("raise"), titleColor: token("ink"), bodyColor: ink2, borderColor: token("ring-strong"), borderWidth: 1,
          padding: 10, boxWidth: 8, boxHeight: 8, boxPadding: 4, usePointStyle: true, cornerRadius: 8,
          titleFont: { weight: 600 }, bodyFont: { size: 12 },
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

onMounted(draw);
onBeforeUnmount(() => chart?.destroy());
// The theme attribute is set first; then the tokens are read again.
watch(theme, () => nextTick(draw));
watch(() => [props.labels, props.series], () => {
  if (!chart || chart.data.datasets.length !== props.series.length) return draw();
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

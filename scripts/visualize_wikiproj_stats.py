import json
import os


INPUT_PATH = os.environ.get("INPUT_PATH", "data/pair_filter_matrix.json")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data/pair_filter_matrix_report.html")


with open(INPUT_PATH, "r", encoding="utf-8") as input_file:
    stats = json.load(input_file)

embedded_stats = json.dumps(stats, ensure_ascii=False).replace("</", "<\\/")

html = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Wikidata Filter Matrix</title>
<style>
:root {
  color-scheme: dark;
  --bg: #0b1020;
  --panel: #121a2d;
  --panel-2: #18233b;
  --text: #edf3ff;
  --muted: #9caccc;
  --line: #293652;
  --accent: #65d6ad;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: radial-gradient(circle at top left, #162744 0, var(--bg) 38%);
  color: var(--text);
  font: 14px/1.45 system-ui, sans-serif;
}
main { max-width: 1600px; margin: auto; padding: 28px; }
h1 { margin: 0; font-size: clamp(28px, 4vw, 48px); }
h2 { margin: 0 0 16px; font-size: 20px; }
p { color: var(--muted); }
.controls, .cards, .layout { display: grid; gap: 14px; }
.controls {
  grid-template-columns: repeat(auto-fit, minmax(210px, max-content));
  align-items: end;
  margin: 24px 0;
}
label { color: var(--muted); font-weight: 650; }
select {
  display: block;
  width: 100%;
  margin-top: 5px;
  padding: 9px 32px 9px 10px;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--panel);
  color: var(--text);
}
.cards { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }
.card, section {
  background: color-mix(in srgb, var(--panel) 92%, transparent);
  border: 1px solid var(--line);
  border-radius: 14px;
  box-shadow: 0 12px 35px #0004;
}
.card { padding: 16px; }
.card .label { color: var(--muted); font-size: 12px; text-transform: uppercase; }
.card .value { margin-top: 4px; font-size: 24px; font-weight: 750; }
.layout { grid-template-columns: minmax(320px, 0.7fr) minmax(500px, 1.3fr); margin-top: 18px; }
section { min-width: 0; padding: 18px; }
.bars { display: grid; gap: 8px; max-height: 720px; overflow: auto; padding-right: 5px; }
.bar-row { display: grid; grid-template-columns: minmax(150px, 1fr) 2fr 95px; gap: 9px; align-items: center; }
.bar-label { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.bar-track { height: 13px; border-radius: 99px; background: #0a1020; overflow: hidden; }
.bar-fill { height: 100%; border-radius: inherit; background: linear-gradient(90deg, #5d8cff, var(--accent)); }
.bar-value { color: var(--muted); text-align: right; font-variant-numeric: tabular-nums; }
.heatmap-wrap { max-height: 760px; overflow: auto; border: 1px solid var(--line); border-radius: 9px; }
table { border-collapse: separate; border-spacing: 1px; background: var(--line); }
th, td { background: var(--panel); }
th {
  position: sticky;
  z-index: 2;
  min-width: 30px;
  max-width: 30px;
  height: 150px;
  padding: 5px;
  color: var(--muted);
  font-size: 10px;
  font-weight: 600;
  vertical-align: bottom;
  writing-mode: vertical-rl;
  transform: rotate(180deg);
}
th:first-child {
  left: 0;
  z-index: 4;
  min-width: 155px;
  writing-mode: horizontal-tb;
  transform: none;
}
thead th { top: 0; }
tbody th {
  left: 0;
  height: auto;
  min-width: 155px;
  max-width: 155px;
  writing-mode: horizontal-tb;
  transform: none;
  text-align: right;
}
td {
  min-width: 30px;
  height: 30px;
  cursor: crosshair;
  transition: outline .1s ease;
}
td:hover { outline: 2px solid white; z-index: 1; }
.legend { display: flex; gap: 8px; align-items: center; margin: 12px 0 0; color: var(--muted); }
.gradient { width: 180px; height: 12px; border-radius: 99px; background: linear-gradient(90deg, #15213a, #30628d, #49b18d, #f3dd75); }
.tooltip {
  position: fixed;
  display: none;
  pointer-events: none;
  max-width: 320px;
  padding: 10px;
  border: 1px solid #ffffff40;
  border-radius: 8px;
  background: #050914ed;
  box-shadow: 0 8px 30px #0009;
  z-index: 20;
}
.tooltip strong { color: var(--accent); }
@media (max-width: 1050px) { .layout { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<main>
  <h1>Wikidata filter matrix</h1>
  <p>Explore basic-filtered coverage and pairwise intersections. Every non-basic filter includes the basic filter.</p>

  <div class="controls">
    <label>Scope<select id="scope"></select></label>
    <label>Heatmap metric
      <select id="metric">
        <option value="containment">Containment: intersection / smaller filter</option>
        <option value="jaccard">Jaccard similarity</option>
        <option value="scope">Intersection / scope total</option>
        <option value="count">Raw intersection count</option>
      </select>
    </label>
    <label>Bar order
      <select id="order">
        <option value="count">Largest first</option>
        <option value="source">Matrix order</option>
      </select>
    </label>
  </div>

  <div class="cards">
    <div class="card"><div class="label">Entities in scope</div><div class="value" id="total"></div></div>
    <div class="card"><div class="label">Items</div><div class="value" id="items"></div></div>
    <div class="card"><div class="label">Properties</div><div class="value" id="properties"></div></div>
    <div class="card"><div class="label">Filters</div><div class="value" id="filters"></div></div>
  </div>

  <div class="layout">
    <section>
      <h2>Individual filter coverage</h2>
      <div class="bars" id="bars"></div>
    </section>
    <section>
      <h2>Pairwise intersection heatmap</h2>
      <div class="heatmap-wrap"><table id="heatmap"></table></div>
      <div class="legend"><span>Low</span><span class="gradient"></span><span>High</span></div>
    </section>
  </div>
</main>
<div class="tooltip" id="tooltip"></div>
<script>
const stats = __STATS__;
const names = stats.filter_order;
const descriptions = Object.fromEntries(stats.filters.map(filter => [filter.id, filter.description]));
const scopes = {all_wikidata: stats.all_wikidata, ...stats.per_language};
const formatter = new Intl.NumberFormat();
const percent = new Intl.NumberFormat(undefined, {style: "percent", maximumFractionDigits: 2});
const friendly = name => name.replace(/^has_/, "").replace(/^not_/, "not ").replaceAll("_", " ");

const scopeSelect = document.querySelector("#scope");
scopeSelect.innerHTML = `<option value="all_wikidata">All Wikidata</option>` +
  stats.target_languages.map(lang => `<option value="${lang}">${lang} or mul label</option>`).join("");

function metricValue(scope, row, col, metric) {
  const intersection = scope.pair_matrix[row][col];
  const rowCount = scope.pair_matrix[row][row];
  const colCount = scope.pair_matrix[col][col];
  if (metric === "count") return intersection;
  if (metric === "scope") return scope.total ? intersection / scope.total : 0;
  if (metric === "jaccard") {
    const union = rowCount + colCount - intersection;
    return union ? intersection / union : 0;
  }
  const smaller = Math.min(rowCount, colCount);
  return smaller ? intersection / smaller : 0;
}

function color(value, metric, maximum) {
  const normalized = metric === "count" ? (maximum ? Math.log1p(value) / Math.log1p(maximum) : 0) : value;
  const hue = 215 - normalized * 165;
  return `hsl(${hue} ${35 + normalized * 45}% ${15 + normalized * 52}%)`;
}

function render() {
  const scope = scopes[scopeSelect.value];
  const metric = document.querySelector("#metric").value;
  document.querySelector("#total").textContent = formatter.format(scope.total);
  document.querySelector("#items").textContent = formatter.format(scope.items);
  document.querySelector("#properties").textContent = formatter.format(scope.properties);
  document.querySelector("#filters").textContent = names.length;

  let bars = names.map((name, index) => ({name, index, count: scope.filter_counts[name]}));
  if (document.querySelector("#order").value === "count") bars.sort((a, b) => b.count - a.count);
  document.querySelector("#bars").innerHTML = bars.map(bar => `
    <div class="bar-row" title="${descriptions[bar.name]}">
      <span class="bar-label">${friendly(bar.name)}</span>
      <span class="bar-track"><span class="bar-fill" style="display:block;width:${scope.total ? bar.count / scope.total * 100 : 0}%"></span></span>
      <span class="bar-value">${percent.format(scope.total ? bar.count / scope.total : 0)}</span>
    </div>`).join("");

  let maximum = 0;
  for (let row = 0; row < names.length; row++) {
    for (let col = 0; col < names.length; col++) {
      maximum = Math.max(maximum, metricValue(scope, row, col, metric));
    }
  }
  const header = `<thead><tr><th>Filter</th>${names.map(name => `<th title="${descriptions[name]}">${friendly(name)}</th>`).join("")}</tr></thead>`;
  const body = names.map((name, row) => `<tr><th title="${descriptions[name]}">${friendly(name)}</th>` +
    names.map((other, col) => {
      const value = metricValue(scope, row, col, metric);
      return `<td data-row="${row}" data-col="${col}" data-value="${value}" style="background:${color(value, metric, maximum)}"></td>`;
    }).join("") + `</tr>`).join("");
  document.querySelector("#heatmap").innerHTML = header + `<tbody>${body}</tbody>`;
}

document.querySelectorAll("select").forEach(select => select.addEventListener("change", render));
const tooltip = document.querySelector("#tooltip");
document.querySelector("#heatmap").addEventListener("mousemove", event => {
  const cell = event.target.closest("td");
  if (!cell) return tooltip.style.display = "none";
  const scope = scopes[scopeSelect.value];
  const metric = document.querySelector("#metric").value;
  const row = Number(cell.dataset.row);
  const col = Number(cell.dataset.col);
  const intersection = scope.pair_matrix[row][col];
  const value = Number(cell.dataset.value);
  const renderedMetric = metric === "count" ? formatter.format(value) : percent.format(value);
  tooltip.innerHTML = `<strong>${friendly(names[row])}</strong> + <strong>${friendly(names[col])}</strong><br>` +
    `Intersection: ${formatter.format(intersection)}<br>${metric}: ${renderedMetric}`;
  tooltip.style.display = "block";
  tooltip.style.left = Math.min(event.clientX + 14, innerWidth - tooltip.offsetWidth - 10) + "px";
  tooltip.style.top = Math.min(event.clientY + 14, innerHeight - tooltip.offsetHeight - 10) + "px";
});
document.querySelector("#heatmap").addEventListener("mouseleave", () => tooltip.style.display = "none");
render();
</script>
</body>
</html>
""".replace("__STATS__", embedded_stats)

output_dir = os.path.dirname(OUTPUT_PATH)
if output_dir:
    os.makedirs(output_dir, exist_ok=True)

with open(OUTPUT_PATH, "w", encoding="utf-8") as output_file:
    output_file.write(html)

print(f"Wrote {OUTPUT_PATH}")

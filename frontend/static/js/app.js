const state = {
  dashboard: null,
  standings: null,
  messages: [],
  activeAnalysisIndex: -1,
  currentView: "analyst",
  loading: false,
  refreshing: false,
};

const elements = {
  navAnalystDesk: document.getElementById("nav-analyst-desk"),
  navStandings: document.getElementById("nav-standings"),
  analystView: document.getElementById("analyst-view"),
  standingsView: document.getElementById("standings-view"),
  chatRail: document.getElementById("chat-rail"),
  heroEyebrow: document.getElementById("hero-eyebrow"),
  heroTitle: document.getElementById("hero-title"),
  heroDescription: document.getElementById("hero-description"),
  metricGrid: document.getElementById("metric-grid"),
  leagueSnapshot: document.getElementById("league-snapshot"),
  promptChips: document.getElementById("prompt-chips"),
  analysisContent: document.getElementById("analysis-content"),
  chatLog: document.getElementById("chat-log"),
  chatForm: document.getElementById("chat-form"),
  chatInput: document.getElementById("chat-input"),
  sendButton: document.getElementById("send-button"),
  standingsPulseTitle: document.getElementById("standings-pulse-title"),
  standingsPulseSummary: document.getElementById("standings-pulse-summary"),
  standingsPulseGrid: document.getElementById("standings-pulse-grid"),
  standingsTitle: document.getElementById("standings-title"),
  standingsSeasonBadge: document.getElementById("standings-season-badge"),
  standingsCountry: document.getElementById("standings-country"),
  standingsLeague: document.getElementById("standings-league"),
  standingsTable: document.getElementById("standings-table"),
  refreshDataButton: document.getElementById("refresh-data-button"),
  refreshStatusBadge: document.getElementById("refresh-status-badge"),
};

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatInlineRichText(value) {
  const escaped = escapeHtml(value);
  return escaped.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
}

function stripMarkdown(value) {
  return String(value || "").replace(/\*\*(.+?)\*\*/g, "$1").trim();
}

function formatDecimal(value, digits = 1, fallback = "--") {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric.toFixed(digits) : fallback;
}

function renderMetricCards(metrics) {
  if (!elements.metricGrid) return;
  elements.metricGrid.innerHTML = metrics
    .map(
      (item) => `
        <article class="metric-card">
          <div class="metric-card-label">${escapeHtml(item.label)}</div>
          <div class="metric-card-value">${escapeHtml(item.value)}</div>
          <div class="metric-card-caption">${escapeHtml(item.caption)}</div>
        </article>
      `,
    )
    .join("");
}

function renderLeagueSnapshot(snapshot) {
  if (!elements.leagueSnapshot) return;
  elements.leagueSnapshot.innerHTML = snapshot
    .map(
      (row) => `
        <article class="league-row">
          <div class="league-row-head">
            <div class="league-name">${escapeHtml(row.country ? `${row.country} · ${row.league}` : row.league)}</div>
            <div class="tool-tag">${escapeHtml(`${formatDecimal(row.home_win_rate, 1)}% home`)}</div>
          </div>
          <div class="league-meta">
            <span>${escapeHtml(`${formatDecimal(row.avg_goals, 2)} goals`)}</span>
            <span>${escapeHtml(`${formatDecimal(row.avg_shots, 1)} shots`)}</span>
            <span>${escapeHtml(`${formatDecimal(row.avg_cards, 2)} cards`)}</span>
          </div>
        </article>
      `,
    )
    .join("");
}

function promptChipButton(prompt) {
  return `<button class="prompt-chip" type="button" data-prompt="${escapeHtml(prompt)}">${escapeHtml(prompt)}</button>`;
}

function renderPromptChips(prompts) {
  if (!elements.promptChips) return;
  elements.promptChips.innerHTML = prompts.map(promptChipButton).join("");
}

function renderToolCalls(toolCalls = []) {
  if (!toolCalls.length) return "";
  const items = toolCalls
    .map(
      (tool) => `
        <div class="tool-call-item">
          <div class="tool-call-name">${escapeHtml(tool.label)}</div>
          <div class="tool-call-summary">${escapeHtml(tool.summary)}</div>
        </div>
      `,
    )
    .join("");
  return `<div class="tool-call-list">${items}</div>`;
}

function renderHighlights(highlights = []) {
  if (!highlights.length) return "";
  const items = highlights
    .map(
      (item) => `
        <div class="highlight-card">
          <div class="metric-card-label">${escapeHtml(item.label)}</div>
          <div class="highlight-value">${escapeHtml(item.value)}</div>
          <div class="highlight-caption">${escapeHtml(item.caption)}</div>
        </div>
      `,
    )
    .join("");
  return `<div class="message-highlights">${items}</div>`;
}

function renderHypothesis(hypothesis) {
  if (!hypothesis || !hypothesis.statement) return "";
  const evidence = (hypothesis.evidence || [])
    .slice(0, 2)
    .map((item) => `<li>${escapeHtml(item)}</li>`)
    .join("");
  const candidates = (hypothesis.candidates || [])
    .slice(0, 2)
    .map(
      (item, index) => `
        <article class="hypothesis-candidate">
          <div class="metric-card-label">Candidate ${index + 1} · ${escapeHtml(item.confidence || "Confidence not set")}</div>
          <div class="highlight-value">${escapeHtml(item.title || "Ranked explanation")}</div>
          <p>${escapeHtml(item.statement || "")}</p>
          ${item.caveats?.length ? `<p><strong>Caveat:</strong> ${escapeHtml(item.caveats[0])}</p>` : ""}
        </article>
      `,
    )
    .join("");
  const footer = [
    hypothesis.correlation_note ? `<p><strong>Correlation vs causation:</strong> ${escapeHtml(hypothesis.correlation_note)}</p>` : "",
    hypothesis.confidence ? `<p><strong>Overall confidence:</strong> ${escapeHtml(hypothesis.confidence)}</p>` : "",
    // hypothesis.next_checks?.length ? `<p><strong>Next check:</strong> ${escapeHtml(hypothesis.next_checks[0])}</p>` : "",
  ].join("");
  return `
    <section class="hypothesis-card">
      <div class="section-kicker">Hypothesis</div>
      <h4>${escapeHtml(hypothesis.title || "Data-backed hypothesis")}</h4>
      <p>${escapeHtml(hypothesis.statement)}</p>
      ${evidence ? `<ul class="hypothesis-evidence">${evidence}</ul>` : ""}
      ${candidates ? `<div class="hypothesis-candidate-stack">${candidates}</div>` : ""}
      ${footer}
    </section>
  `;
}

function renderExecutiveSummary(points = [], fallbackText = "") {
  const items = points.length
    ? points.map((point) => `<li>${formatInlineRichText(point)}</li>`).join("")
    : `<li>${formatInlineRichText(fallbackText)}</li>`;
  return `<ul class="executive-summary-list">${items}</ul>`;
}

function normalizeSeriesData(series = []) {
  return series.map((item, index) => ({
    name: item.name || `Series ${index + 1}`,
    color: item.color || ["#4ad9c6", "#4a67ff", "#f3d26f", "#ff6579"][index % 4],
    data: (item.data || []).map((value) => {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : null;
    }),
  }));
}

function renderLineChart(chart) {
  const width = 640;
  const height = 230;
  const padding = { top: 16, right: 18, bottom: 34, left: 42 };
  const series = normalizeSeriesData(chart.series);
  const xValues = chart.x || [];
  const allValues = series.flatMap((item) => item.data).filter((value) => value !== null);
  if (!xValues.length || !allValues.length) return "";

  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);
  const yMin = minValue === maxValue ? minValue - 1 : minValue;
  const yMax = minValue === maxValue ? maxValue + 1 : maxValue;
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const xStep = xValues.length > 1 ? plotWidth / (xValues.length - 1) : 0;
  const yScale = (value) => padding.top + plotHeight - ((value - yMin) / (yMax - yMin)) * plotHeight;

  const lines = series
    .map((item) => {
      const points = item.data
        .map((value, index) => (value === null ? null : `${index === 0 ? "M" : "L"} ${padding.left + index * xStep} ${yScale(value)}`))
        .filter(Boolean)
        .join(" ");
      const dots = item.data
        .map((value, index) =>
          value === null
            ? ""
            : `<circle cx="${padding.left + index * xStep}" cy="${yScale(value)}" r="3.2" fill="${item.color}"></circle>`,
        )
        .join("");
      return `<path d="${points}" fill="none" stroke="${item.color}" stroke-width="2.6" stroke-linecap="round"></path>${dots}`;
    })
    .join("");

  const yTicks = Array.from({ length: 4 }, (_, index) => {
    const value = yMin + ((yMax - yMin) * index) / 3;
    const y = yScale(value);
    return `
      <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="chart-grid-line"></line>
      <text x="${padding.left - 8}" y="${y + 4}" text-anchor="end" class="chart-axis-label">${escapeHtml(formatDecimal(value, 1))}</text>
    `;
  }).join("");

  const labelStep = Math.max(1, Math.ceil(xValues.length / 5));
  const xLabels = xValues
    .map((label, index) => {
      if (index % labelStep !== 0 && index !== xValues.length - 1) return "";
      return `<text x="${padding.left + index * xStep}" y="${height - 8}" text-anchor="middle" class="chart-axis-label">${escapeHtml(label)}</text>`;
    })
    .join("");

  const legend = series
    .map(
      (item) => `
        <span class="chart-legend-item">
          <span class="chart-legend-swatch" style="background:${item.color}"></span>
          ${escapeHtml(item.name)}
        </span>
      `,
    )
    .join("");

  return `
    <article class="chart-card">
      <div class="chart-card-head">
        <h4>${escapeHtml(chart.title)}</h4>
        <div class="chart-legend">${legend}</div>
      </div>
      <svg viewBox="0 0 ${width} ${height}" class="chart-svg" role="img" aria-label="${escapeHtml(chart.title)}">
        ${yTicks}
        ${lines}
        ${xLabels}
      </svg>
      <p class="chart-summary">${escapeHtml(chart.summary || "")}</p>
    </article>
  `;
}

function renderBarChart(chart) {
  const width = 640;
  const height = 240;
  const padding = { top: 18, right: 18, bottom: 52, left: 42 };
  const series = normalizeSeriesData(chart.series);
  const categories = chart.x || [];
  const allValues = series.flatMap((item) => item.data).filter((value) => value !== null);
  if (!categories.length || !allValues.length) return "";

  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const groupWidth = plotWidth / categories.length;
  const maxValue = Math.max(...allValues, 0.1);
  const yScale = (value) => padding.top + plotHeight - (value / maxValue) * plotHeight;
  const seriesCount = Math.max(1, series.length);
  const innerGroupWidth = Math.max(12, groupWidth * 0.72);
  const barWidth = Math.max(8, innerGroupWidth / seriesCount - 4);

  const bars = categories
    .map((category, categoryIndex) => {
      const baseX = padding.left + categoryIndex * groupWidth + (groupWidth - innerGroupWidth) / 2;
      const label = category.length > 18 ? `${category.slice(0, 16)}…` : category;
      const rects = series
        .map((item, seriesIndex) => {
          const value = item.data[categoryIndex];
          if (value === null) return "";
          const x = baseX + seriesIndex * (barWidth + 4);
          const y = yScale(value);
          const heightValue = padding.top + plotHeight - y;
          return `<rect x="${x}" y="${y}" width="${barWidth}" height="${heightValue}" rx="5" fill="${item.color}"></rect>`;
        })
        .join("");
      return `
        ${rects}
        <text x="${padding.left + categoryIndex * groupWidth + groupWidth / 2}" y="${height - 16}" text-anchor="middle" class="chart-axis-label">${escapeHtml(label)}</text>
      `;
    })
    .join("");

  const yTicks = Array.from({ length: 4 }, (_, index) => {
    const value = (maxValue * index) / 3;
    const y = yScale(value);
    return `
      <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="chart-grid-line"></line>
      <text x="${padding.left - 8}" y="${y + 4}" text-anchor="end" class="chart-axis-label">${escapeHtml(formatDecimal(value, 1))}</text>
    `;
  }).join("");

  const legend = series
    .map(
      (item) => `
        <span class="chart-legend-item">
          <span class="chart-legend-swatch" style="background:${item.color}"></span>
          ${escapeHtml(item.name)}
        </span>
      `,
    )
    .join("");

  return `
    <article class="chart-card">
      <div class="chart-card-head">
        <h4>${escapeHtml(chart.title)}</h4>
        <div class="chart-legend">${legend}</div>
      </div>
      <svg viewBox="0 0 ${width} ${height}" class="chart-svg" role="img" aria-label="${escapeHtml(chart.title)}">
        ${yTicks}
        ${bars}
      </svg>
      <p class="chart-summary">${escapeHtml(chart.summary || "")}</p>
    </article>
  `;
}

function heatmapColor(value, minValue, maxValue) {
  if (!Number.isFinite(value)) return "rgba(255,255,255,0.08)";
  const normalized = maxValue === minValue ? 0.5 : (value - minValue) / (maxValue - minValue);
  const hue = 210 - normalized * 160;
  const lightness = 22 + normalized * 40;
  return `hsl(${hue} 78% ${lightness}%)`;
}

function renderHeatmapChart(chart) {
  const rows = chart.rows || [];
  const columns = chart.columns || [];
  const matrix = chart.z || [];
  if (!rows.length || !columns.length || !matrix.length) return "";

  const allValues = matrix.flat().map(Number).filter(Number.isFinite);
  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);

  const body = rows
    .map((rowLabel, rowIndex) => {
      const cells = columns
        .map((columnLabel, columnIndex) => {
          const value = Number(matrix[rowIndex]?.[columnIndex]);
          return `
            <div class="heatmap-cell" style="background:${heatmapColor(value, minValue, maxValue)}">
              <span>${escapeHtml(formatDecimal(value, 2))}</span>
            </div>
          `;
        })
        .join("");
      return `
        <div class="heatmap-row">
          <div class="heatmap-axis-label">${escapeHtml(rowLabel)}</div>
          <div class="heatmap-grid-row">${cells}</div>
        </div>
      `;
    })
    .join("");

  const header = columns
    .map((column) => `<div class="heatmap-column-label">${escapeHtml(column)}</div>`)
    .join("");

  return `
    <article class="chart-card heatmap-card">
      <div class="chart-card-head">
        <h4>${escapeHtml(chart.title)}</h4>
      </div>
      <div class="heatmap-wrapper">
        <div class="heatmap-header-spacer"></div>
        <div class="heatmap-header">${header}</div>
        ${body}
      </div>
      <p class="chart-summary">${escapeHtml(chart.summary || "")}</p>
    </article>
  `;
}

function renderCharts(charts = []) {
  if (!charts.length) return "";
  const items = charts
    .map((chart) => {
      if (chart.type === "line") return renderLineChart(chart);
      if (chart.type === "bar") return renderBarChart(chart);
      if (chart.type === "heatmap") return renderHeatmapChart(chart);
      return "";
    })
    .join("");
  return `<div class="chart-stack">${items}</div>`;
}

function renderTable(table) {
  if (!table || !table.columns?.length || !table.rows?.length) return "";
  const head = table.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  const body = table.rows
    .map(
      (row) => `
        <tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>
      `,
    )
    .join("");
  return `
    <div class="message-table">
      <table>
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function renderMessageSuggestions(suggestions = []) {
  if (!suggestions.length) return "";
  const buttons = suggestions
    .map(
      (prompt) => `<button class="message-suggestion" type="button" data-prompt="${escapeHtml(prompt)}">${escapeHtml(prompt)}</button>`,
    )
    .join("");
  return `<div class="message-suggestions">${buttons}</div>`;
}

function renderSources(sources = []) {
  if (!sources.length) return "";
  const items = sources
    .slice(0, 3)
    .map((source) => {
      const title = escapeHtml(source.title || "Source");
      const snippet = escapeHtml(source.snippet || "");
      const meta = escapeHtml(source.source_type || "source");
      const link = source.url
        ? `<a href="${escapeHtml(source.url)}" target="_blank" rel="noopener noreferrer">${title}</a>`
        : title;
      return `
        <li class="source-item">
          <div class="source-title">${link}</div>
          <div class="source-meta">${meta}</div>
          ${snippet ? `<div class="source-snippet">${snippet}</div>` : ""}
        </li>
      `;
    })
    .join("");
  return `
    <section class="source-card">
      <div class="section-kicker">External Validation Links</div>
      <ul class="source-list">${items}</ul>
    </section>
  `;
}

function titleFromQuestion(question, fallback = "Football analyst result") {
  const value = String(question || "").trim();
  return value || fallback;
}

function previewText(text, limit = 150) {
  const normalized = String(text || "").trim();
  if (normalized.length <= limit) return normalized;
  return `${normalized.slice(0, limit - 1)}…`;
}

function loadingMarkup() {
  return `
    <div class="loading-dot" aria-label="Loading">
      <span></span>
      <span></span>
      <span></span>
    </div>
  `;
}

function resultBadge(result) {
  const normalized = String(result || "").toUpperCase();
  const labelMap = { W: "Win", D: "Draw", L: "Loss" };
  return `
    <span class="form-pill form-pill-${normalized.toLowerCase()}" title="${escapeHtml(labelMap[normalized] || normalized)}">
      ${escapeHtml(normalized)}
    </span>
  `;
}

function setActiveView(viewName) {
  state.currentView = viewName;
  elements.analystView?.classList.toggle("hidden-view", viewName !== "analyst");
  elements.standingsView?.classList.toggle("hidden-view", viewName !== "standings");
  elements.navAnalystDesk?.classList.toggle("pill-active", viewName === "analyst");
  elements.navStandings?.classList.toggle("pill-active", viewName === "standings");
  elements.chatRail?.classList.toggle("hidden-view", viewName === "standings");
  document.body.classList.toggle("standings-mode", viewName === "standings");
}

function renderStandingsPulse(pulse) {
  if (!elements.standingsPulseGrid) return;
  elements.standingsPulseTitle.textContent = pulse?.title || "League Pulse";
  elements.standingsPulseSummary.textContent = pulse?.summary || "";
  elements.standingsPulseGrid.innerHTML = (pulse?.metrics || [])
    .map(
      (item) => `
        <article class="metric-card standings-pulse-card">
          <div class="metric-card-label">${escapeHtml(item.label)}</div>
          <div class="metric-card-value">${escapeHtml(item.value)}</div>
          <div class="metric-card-caption">${escapeHtml(item.caption)}</div>
        </article>
      `,
    )
    .join("");
}

function renderStandings(payload) {
  if (!elements.standingsTable) return;
  state.standings = payload;
  renderStandingsPulse(payload.pulse);
  elements.standingsTitle.textContent = `${payload.selected_country} · ${payload.selected_league}`;
  elements.standingsSeasonBadge.textContent = payload.selected_season;

  elements.standingsCountry.innerHTML = payload.country_options
    .map(
      (country) =>
        `<option value="${escapeHtml(country)}"${country === payload.selected_country ? " selected" : ""}>${escapeHtml(country)}</option>`,
    )
    .join("");

  elements.standingsLeague.innerHTML = payload.league_options
    .map(
      (league) =>
        `<option value="${escapeHtml(league)}"${league === payload.selected_league ? " selected" : ""}>${escapeHtml(league)}</option>`,
    )
    .join("");

  if (!payload.rows.length) {
    elements.standingsTable.innerHTML = `<div class="standings-empty">No completed match data available yet for this selection.</div>`;
    return;
  }

  const body = payload.rows
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(row.rank)}</td>
          <td class="standings-club">${escapeHtml(row.club)}</td>
          <td>${escapeHtml(row.mp)}</td>
          <td>${escapeHtml(row.w)}</td>
          <td>${escapeHtml(row.d)}</td>
          <td>${escapeHtml(row.l)}</td>
          <td>${escapeHtml(row.gf)}</td>
          <td>${escapeHtml(row.ga)}</td>
          <td>${escapeHtml(row.gd)}</td>
          <td>${escapeHtml(row.pts)}</td>
          <td><div class="form-strip">${row.last5.map(resultBadge).join("")}</div></td>
        </tr>
      `,
    )
    .join("");

  elements.standingsTable.innerHTML = `
    <table class="standings-table">
      <thead>
        <tr>
          <th>Rank</th>
          <th>Club</th>
          <th>MP</th>
          <th>W</th>
          <th>D</th>
          <th>L</th>
          <th>GF</th>
          <th>GA</th>
          <th>GD</th>
          <th>Pts</th>
          <th>Last 5</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

async function loadStandings(country = "", league = "") {
  const params = new URLSearchParams();
  if (country) params.set("country", country);
  if (league) params.set("league", league);

  const response = await fetch(`/standings?${params.toString()}`);
  if (!response.ok) {
    throw new Error("Failed to load standings.");
  }

  renderStandings(await response.json());
}

function activeAnalysisMessage() {
  const message = state.messages[state.activeAnalysisIndex];
  if (!message || message.role !== "assistant") return null;
  return message;
}

function renderAnalysisCanvas() {
  const message = activeAnalysisMessage();
  if (!message) {
    const suggestions = (state.dashboard?.prompt_chips || [])
      .map(promptChipButton)
      .join("");
    elements.analysisContent.innerHTML = `
      <div class="analysis-empty">
        <p class="section-kicker">Analyst desk</p>
        <h3>Run a football question to open the full analysis canvas.</h3>
        <p>The result view will show the answer, EDA evidence, charts, table output, and source trail outside the chat history.</p>
        <div class="prompt-chips prompt-chips-compact">${suggestions}</div>
      </div>
    `;
    return;
  }

  const title = titleFromQuestion(message.question);
  const metaChips = (message.isConversational || message.isSimpleResponse)
    ? ""
    : [
      message.scope ? `<span class="analysis-chip">${escapeHtml(message.scope)}</span>` : "",
      message.dataMode && message.dataMode !== "none" ? `<span class="analysis-chip">${escapeHtml(message.dataMode.replaceAll("_", " "))}</span>` : "",
      message.provider ? `<span class="analysis-chip">${escapeHtml(message.provider)}</span>` : "",
    ]
      .filter(Boolean)
      .join("");

  const supportingTable = message.table
    ? `
      <section class="analysis-section-card">
        <div class="section-kicker">Supporting Table</div>
        ${renderTable(message.table)}
      </section>
    `
    : "";

  const minimalResponse = message.outOfContext || message.isConversational || message.isSimpleResponse;
  const outOfContextExtras = minimalResponse
    ? ""
    : `
    ${message.loading ? "" : renderHighlights(message.highlights)}
    ${message.loading ? "" : renderHypothesis(message.hypothesis)}
    ${message.loading ? "" : renderCharts(message.charts)}
    ${message.loading ? "" : supportingTable}
    ${message.loading ? "" : renderSources(message.sources)}
    ${message.loading ? "" : renderMessageSuggestions(message.suggestions)}
  `;

  const simpleBody = (message.isConversational || message.isSimpleResponse)
    ? `
    <section class="analysis-section-card">
      <div class="section-kicker">${escapeHtml(message.isConversational ? "Reply" : "Answer")}</div>
      <div class="analysis-answer-text">${escapeHtml(message.text)}</div>
      ${message.suggestions?.length ? renderMessageSuggestions(message.suggestions) : ""}
    </section>
  `
    : "";

  const standardSummary = (message.isConversational || message.isSimpleResponse)
    ? ""
    : `
    <section class="analysis-summary-card">
      <div class="section-kicker">Executive Summary</div>
      <div class="analysis-answer-text">
        ${message.loading ? loadingMarkup() : renderExecutiveSummary(message.executiveSummary, message.text)}
      </div>
    </section>
  `;

  elements.analysisContent.innerHTML = `
    <section class="analysis-hero-card">
      <div>
        <p class="section-kicker">${escapeHtml(message.isConversational ? "Conversation" : (message.isSimpleResponse ? "Football knowledge" : (message.outOfContext ? "Out of context" : "Curated result")))}</p>
        <h2>${escapeHtml(title)}</h2>
        <div class="analysis-meta-row">${metaChips}</div>
      </div>
    </section>
    ${standardSummary}
    ${simpleBody}
    ${outOfContextExtras}
  `;
}

function renderMessages() {
  const visibleMessages = state.messages.filter((message) => message.role === "user" || message.loading);
  elements.chatLog.innerHTML = visibleMessages
    .map((message, index) => {
      const content = message.loading
        ? "Running analysis..."
        : previewText(message.text, 110);
      const secondary = message.loading ? "Preparing result on the left" : "Submitted";
      const activeClass = message.loading ? "active" : "";
      return `
        <article class="message history-message ${escapeHtml(message.role)} ${activeClass}">
          <div class="message-bubble">
            <div class="message-meta">
              <div class="message-role">${escapeHtml(message.loading ? "Running" : "You")}</div>
            </div>
            <div class="message-body">${content}</div>
            <div class="history-secondary">${escapeHtml(secondary)}</div>
          </div>
        </article>
      `;
    })
    .join("");

  elements.chatLog.scrollTop = elements.chatLog.scrollHeight;
}

function pushMessage(message) {
  state.messages.push(message);
  if (message.role === "assistant") {
    state.activeAnalysisIndex = state.messages.length - 1;
  }
  renderAnalysisCanvas();
  renderMessages();
}

function replaceLoadingMessage(nextMessage) {
  const index = state.messages.findIndex((message) => message.loading);
  if (index >= 0) {
    state.messages[index] = nextMessage;
    state.activeAnalysisIndex = index;
  } else {
    state.messages.push(nextMessage);
    state.activeAnalysisIndex = state.messages.length - 1;
  }
  renderAnalysisCanvas();
  renderMessages();
}

async function loadDashboard() {
  const response = await fetch("/stats");
  if (!response.ok) {
    throw new Error("Failed to load dashboard.");
  }

  const payload = await response.json();
  state.dashboard = payload;

  elements.heroEyebrow.textContent = "Analyst desk";
  elements.heroTitle.textContent = "Latest result";
  elements.heroDescription.textContent = "Answer, charts, and sources.";

  renderMetricCards(payload.metrics);
  renderLeagueSnapshot(payload.league_snapshot);
  renderPromptChips(payload.prompt_chips);
}

async function sendMessage(message) {
  const response = await fetch("/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ message }),
  });

  if (!response.ok) {
    let detail = "Failed to get analyst response.";
    try {
      const payload = await response.json();
      if (typeof payload.detail === "string") {
        detail = payload.detail;
      } else if (payload.detail?.message) {
        detail = payload.detail.message;
      }
    } catch (error) {
      detail = detail;
    }
    throw new Error(detail);
  }

  return response.json();
}

async function handleChatSubmit(event) {
  event.preventDefault();
  const value = elements.chatInput.value.trim();
  if (!value || state.loading) return;

  state.loading = true;
  elements.sendButton.disabled = true;
  pushMessage({
    role: "user",
    text: value,
    toolCalls: [],
    highlights: [],
    hypothesis: null,
    charts: [],
    table: null,
    sources: [],
    executiveSummary: [],
    suggestions: [],
  });
  pushMessage({
    role: "assistant",
    text: "",
    question: value,
    loading: true,
    toolCalls: [],
    highlights: [],
    hypothesis: null,
    charts: [],
    table: null,
    sources: [],
    executiveSummary: [],
    scope: "",
    dataMode: "running",
    outOfContext: false,
    provider: "",
    suggestions: [],
  });

  elements.chatInput.value = "";

  try {
    const payload = await sendMessage(value);
    replaceLoadingMessage({
      role: "assistant",
      text: payload.answer,
      executiveSummary: payload.executive_summary || [],
      question: value,
      toolCalls: [
        ...(payload.tool_calls || []),
        ...(payload.fallback_used
          ? [{ label: "Model Fallback", summary: "Used deterministic analyst output because the configured model was unavailable." }]
          : [{ label: payload.provider, summary: `Synthesized the final answer with ${payload.model}.` }]),
      ],
      highlights: payload.highlights,
      hypothesis: payload.hypothesis,
      charts: payload.charts,
      table: payload.table,
      sources: payload.sources,
      scope: payload.scope,
      dataMode: payload.data_mode,
      outOfContext: payload.out_of_context,
      isConversational: payload.is_conversational,
      isSimpleResponse: payload.is_simple_response,
      provider: payload.provider,
      suggestions: payload.suggested_prompts,
    });
  } catch (error) {
    replaceLoadingMessage({
      role: "assistant",
      text: error.message,
      executiveSummary: [],
      question: value,
      toolCalls: [],
      highlights: [],
      hypothesis: null,
      charts: [],
      table: null,
      sources: [],
      scope: "",
      dataMode: "error",
      outOfContext: false,
      isConversational: false,
      isSimpleResponse: false,
      provider: "",
      suggestions: state.dashboard?.prompt_chips || [],
    });
  } finally {
    state.loading = false;
    elements.sendButton.disabled = false;
    elements.chatInput.focus();
  }
}

function delegatePromptClicks(event) {
  const target = event.target.closest("[data-prompt]");
  if (!target) return;
  const prompt = target.dataset.prompt;
  elements.chatInput.value = prompt;
  elements.chatInput.focus();
}

function delegateHistoryClicks(event) {
  const target = event.target.closest("[data-message-index]");
  if (!target) return;
  const index = Number(target.dataset.messageIndex);
  if (!Number.isFinite(index)) return;
  const message = state.messages[index];
  if (!message || message.role !== "assistant") return;
  state.activeAnalysisIndex = index;
  renderMessages();
  renderAnalysisCanvas();
}

function setRefreshState(isRefreshing, message) {
  state.refreshing = isRefreshing;
  elements.refreshDataButton.disabled = isRefreshing;
  elements.refreshDataButton.textContent = isRefreshing ? "Refreshing..." : "Refresh Data";
  elements.refreshStatusBadge.textContent = message;
}

function wait(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function pollRefreshJob(jobId) {
  while (true) {
    const response = await fetch(`/refresh/${jobId}`);
    if (!response.ok) {
      throw new Error("Failed to fetch refresh status.");
    }

    const payload = await response.json();
    if (payload.status === "queued" || payload.status === "running") {
      await wait(1500);
      continue;
    }
    if (payload.status === "succeeded") {
      return payload;
    }

    const detail = `${payload.detail} ${payload.output_tail?.join(" | ") || ""}`.trim();
    throw new Error(detail || "Refresh failed.");
  }
}

async function refreshData() {
  if (state.refreshing) return;

  setRefreshState(true, "");
  try {
    const response = await fetch("/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });

    if (!response.ok) {
      let detail = "Refresh failed.";
      try {
        const payload = await response.json();
        if (typeof payload.detail === "string") {
          detail = payload.detail;
        } else if (payload.detail?.message) {
          detail = `${payload.detail.message} ${payload.detail.output_tail?.join(" | ") || ""}`.trim();
        }
      } catch (error) {
        detail = detail;
      }
      throw new Error(detail);
    }

    const payload = await response.json();
    const finalStatus = await pollRefreshJob(payload.job_id);
    await loadDashboard();
    if (state.currentView === "standings" || state.standings) {
      await loadStandings(state.standings?.selected_country || "", state.standings?.selected_league || "");
    }
    setRefreshState(false, finalStatus.detail);
  } catch (error) {
    setRefreshState(false, error.message);
  }
}

async function handleStandingsCountryChange() {
  await loadStandings(elements.standingsCountry.value, "");
}

async function handleStandingsLeagueChange() {
  await loadStandings(elements.standingsCountry.value, elements.standingsLeague.value);
}

async function handleStandingsView() {
  setActiveView("standings");
  if (!state.standings) {
    elements.standingsTable.innerHTML = `<div class="standings-empty">Loading standings...</div>`;
    try {
      await loadStandings();
    } catch (error) {
      elements.standingsTable.innerHTML = `<div class="standings-empty">${escapeHtml(error.message)}</div>`;
    }
  }
}

function handleAnalystView() {
  setActiveView("analyst");
}

function handleInitialLoadError(error) {
  renderMetricCards([{ label: "Status", value: "Offline", caption: "Could not load dashboard payload" }]);
  pushMessage({
    role: "assistant",
    text: `${error.message} Start the Python web server and reload the page.`,
    toolCalls: [],
    highlights: [],
    table: null,
    suggestions: ["Analyze La Liga", "Compare Spain leagues on goals and cards"],
  });
}

async function boot() {
  elements.chatForm.addEventListener("submit", handleChatSubmit);
  elements.refreshDataButton.addEventListener("click", refreshData);
  elements.navAnalystDesk?.addEventListener("click", handleAnalystView);
  elements.navStandings?.addEventListener("click", handleStandingsView);
  elements.standingsCountry?.addEventListener("change", handleStandingsCountryChange);
  elements.standingsLeague?.addEventListener("change", handleStandingsLeagueChange);
  document.addEventListener("click", delegatePromptClicks);
  elements.chatLog.addEventListener("click", delegateHistoryClicks);

  try {
    await loadDashboard();
  } catch (error) {
    handleInitialLoadError(error);
  }

  renderAnalysisCanvas();
  setActiveView("analyst");
}

boot();

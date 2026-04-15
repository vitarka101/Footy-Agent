const state = {
  options: null,
  analysis: null,
  forceRefresh: false,
};

const elements = {
  league: document.getElementById("betting-league"),
  season: document.getElementById("betting-season"),
  model: document.getElementById("betting-model"),
  trainPct: document.getElementById("betting-train-pct"),
  trainPctLabel: document.getElementById("betting-train-pct-label"),
  homeTeam: document.getElementById("betting-home-team"),
  awayTeam: document.getElementById("betting-away-team"),
  analyzeButton: document.getElementById("betting-analyze"),
  simulateButton: document.getElementById("betting-simulate"),
  refreshButton: document.getElementById("betting-refresh"),
  sourceSummary: document.getElementById("betting-source-summary"),
  fixtureTitle: document.getElementById("betting-fixture-title"),
  runtimeBadge: document.getElementById("betting-runtime-badge"),
  actualResult: document.getElementById("betting-actual-result"),
  simulatedResult: document.getElementById("betting-simulated-result"),
  probabilities: document.getElementById("betting-probabilities"),
  homeXg: document.getElementById("betting-home-xg"),
  awayXg: document.getElementById("betting-away-xg"),
  homeTeamLabel: document.getElementById("betting-home-team-label"),
  awayTeamLabel: document.getElementById("betting-away-team-label"),
  mostLikelyScore: document.getElementById("betting-most-likely-score"),
  mostLikelyProbability: document.getElementById("betting-most-likely-probability"),
  scoreMatrix: document.getElementById("betting-score-matrix"),
  hypothesis: document.getElementById("betting-hypothesis"),
  tests: document.getElementById("betting-tests"),
  predictedTable: document.getElementById("betting-predicted-table"),
  actualTable: document.getElementById("betting-actual-table"),
  tools: document.getElementById("betting-tools"),
};

const MODELS = ["Maher", "Dixon-Coles", "Dixon-Coles TD", "Bivariate Poisson", "Negative Binomial"];

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderSelect(select, options, selectedValue) {
  select.innerHTML = options
    .map((option) => {
      const selected = option.value === selectedValue ? " selected" : "";
      return `<option value="${escapeHtml(option.value)}"${selected}>${escapeHtml(option.label)}</option>`;
    })
    .join("");
}

function currentRequestBody() {
  return {
    league_id: elements.league.value,
    season: elements.season.value,
    home_team: elements.homeTeam.value,
    away_team: elements.awayTeam.value,
    model: elements.model.value,
    train_pct: Number(elements.trainPct.value),
    force_refresh: state.forceRefresh,
  };
}

async function loadOptions() {
  const params = new URLSearchParams({
    league_id: elements.league.value || "E0",
    season: elements.season.value || "",
  });
  const response = await fetch(`/betting/options?${params.toString()}`);
  if (!response.ok) {
    throw new Error("Failed to load betting room options.");
  }
  const payload = await response.json();
  state.options = payload;
  renderSelect(elements.league, payload.league_options, payload.selected_league);
  renderSelect(elements.season, payload.season_options, payload.selected_season);
  renderSelect(
    elements.model,
    MODELS.map((model) => ({ value: model, label: model })),
    elements.model.value || "Maher",
  );
  renderSelect(elements.homeTeam, payload.team_options, payload.team_options[0]?.value || "");
  const awayOptions = payload.team_options.filter((team) => team.value !== elements.homeTeam.value);
  renderSelect(elements.awayTeam, awayOptions, awayOptions[1]?.value || awayOptions[0]?.value || "");
  elements.sourceSummary.textContent = payload.source_summary;
}

function renderProbabilities(probabilities) {
  const bars = [
    { label: "Home Win", value: probabilities.home, className: "home" },
    { label: "Draw", value: probabilities.draw, className: "draw" },
    { label: "Away Win", value: probabilities.away, className: "away" },
  ];
  elements.probabilities.innerHTML = bars
    .map(
      (bar) => `
        <article class="betting-probability-card betting-probability-card-${bar.className}">
          <div class="betting-probability-fill" style="height:${Math.max(bar.value, 4)}%"></div>
          <strong>${bar.value.toFixed(1)}%</strong>
          <span>${bar.label}</span>
        </article>
      `,
    )
    .join("");
}

function renderMatrix(scoreMatrix) {
  const columns = Array.from({ length: 9 }, (_, index) => `<div class="betting-matrix-header">${index}</div>`).join("");
  const rows = scoreMatrix.rows
    .slice(0, 9)
    .map((row) => {
      const cells = Array.from({ length: 9 }, (_, index) => {
        const value = Number(row[String(index)] ?? 0);
        const tone = row.home_goals > index ? "home" : row.home_goals < index ? "away" : "draw";
        return `<div class="betting-matrix-cell betting-matrix-cell-${tone}" style="opacity:${Math.max(value / 20, 0.12)}">${value.toFixed(1)}</div>`;
      }).join("");
      return `<div class="betting-matrix-header">${row.home_goals}</div>${cells}`;
    })
    .join("");
  elements.scoreMatrix.innerHTML = `
    <div class="betting-matrix">
      <div class="betting-matrix-header betting-matrix-header-corner">H\\A</div>
      ${columns}
      ${rows}
    </div>
  `;
}

function renderHypothesis(payload) {
  const evidence = (payload.evidence || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("");
  const caveats = (payload.caveats || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("");
  elements.hypothesis.innerHTML = `
    <div class="betting-hypothesis-head">
      <div>
        <p class="betting-confidence">Confidence: ${escapeHtml(payload.confidence || "medium")}</p>
        <h3>${escapeHtml(payload.title || "Betting thesis")}</h3>
      </div>
    </div>
    <p class="betting-hypothesis-statement">${escapeHtml(payload.statement || "")}</p>
    <div class="betting-hypothesis-grid">
      <div>
        <h4>Evidence</h4>
        <ul>${evidence}</ul>
      </div>
      <div>
        <h4>Caveats</h4>
        <ul>${caveats}</ul>
      </div>
    </div>
  `;
}

function testCard(label, result, extra = "") {
  if (!result) {
    return `<article class="betting-test-card"><h4>${escapeHtml(label)}</h4><p>No test output.</p></article>`;
  }
  return `
    <article class="betting-test-card">
      <h4>${escapeHtml(label)}</h4>
      <p>Statistic: <strong>${Number(result.statistic || 0).toFixed(2)}</strong></p>
      <p>df: <strong>${escapeHtml(result.df ?? "-")}</strong></p>
      <p>p-value: <strong>${Number(result.p_value || 0).toFixed(4)}</strong></p>
      ${extra ? `<p>${extra}</p>` : ""}
    </article>
  `;
}

function renderTests(assumptions) {
  elements.tests.innerHTML = [
    testCard("Home goals GOF", assumptions.home_goal_gof),
    testCard("Away goals GOF", assumptions.away_goal_gof),
    testCard("Independence", assumptions.independence),
    testCard("Home dispersion", assumptions.home_dispersion, `Variance / mean: ${Number(assumptions.home_dispersion?.ratio || 0).toFixed(3)}`),
    testCard("Away dispersion", assumptions.away_dispersion, `Variance / mean: ${Number(assumptions.away_dispersion?.ratio || 0).toFixed(3)}`),
    `<article class="betting-test-card"><h4>Training Summary</h4><p>Home win rate: <strong>${Number(assumptions.home_win_rate || 0).toFixed(1)}%</strong></p><p>Mean goals: <strong>${Number(assumptions.mean_home_goals || 0).toFixed(2)} / ${Number(assumptions.mean_away_goals || 0).toFixed(2)}</strong></p></article>`,
  ].join("");
}

function renderTable(target, rows) {
  if (!rows?.length) {
    target.innerHTML = `<div class="standings-empty">No rows available.</div>`;
    return;
  }
  target.innerHTML = `
    <table class="standings-table">
      <thead>
        <tr>
          <th>#</th>
          <th>Team</th>
          <th>P</th>
          <th>W</th>
          <th>D</th>
          <th>L</th>
          <th>GF</th>
          <th>GA</th>
          <th>Pts</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (row, index) => `
              <tr>
                <td>${index + 1}</td>
                <td class="standings-club">${escapeHtml(row.team)}</td>
                <td>${row.p}</td>
                <td>${row.w}</td>
                <td>${row.d}</td>
                <td>${row.l}</td>
                <td>${row.gf}</td>
                <td>${row.ga}</td>
                <td>${row.pts}</td>
              </tr>
            `,
          )
          .join("")}
      </tbody>
    </table>
  `;
}

function renderTools(toolCalls) {
  elements.tools.innerHTML = toolCalls
    .map(
      (tool) => `
        <article class="betting-tool-card">
          <div class="betting-tool-head">
            <strong>${escapeHtml(tool.label)}</strong>
            <span>${escapeHtml(tool.function_name || tool.name)}</span>
          </div>
          <p>${escapeHtml(tool.summary)}</p>
          <small>${tool.duration_ms ? `${tool.duration_ms} ms` : ""}</small>
        </article>
      `,
    )
    .join("");
}

function renderAnalysis(payload) {
  state.analysis = payload;
  elements.fixtureTitle.textContent = `${payload.home_team} vs ${payload.away_team}`;
  elements.runtimeBadge.textContent = `${payload.selected_model} · ${payload.data_mode}`;
  elements.actualResult.textContent = payload.actual_result
    ? `${payload.home_team} ${payload.actual_result.home} - ${payload.actual_result.away} ${payload.away_team}`
    : "No actual result row found for this fixture.";
  elements.simulatedResult.textContent = `${payload.home_team} ${payload.simulated_result.home} - ${payload.simulated_result.away} ${payload.away_team}`;
  elements.homeXg.textContent = payload.expected_goals.home.toFixed(2);
  elements.awayXg.textContent = payload.expected_goals.away.toFixed(2);
  elements.homeTeamLabel.textContent = payload.home_team;
  elements.awayTeamLabel.textContent = payload.away_team;
  elements.mostLikelyScore.textContent = `${payload.most_likely_score.home} - ${payload.most_likely_score.away}`;
  elements.mostLikelyProbability.textContent = `${payload.most_likely_score.probability.toFixed(1)}%`;
  renderProbabilities(payload.probabilities);
  renderMatrix(payload.score_matrix);
  renderHypothesis(payload.hypothesis);
  renderTests(payload.assumptions);
  renderTable(elements.predictedTable, payload.predicted_table);
  renderTable(elements.actualTable, payload.actual_table);
  renderTools(payload.tool_calls);
  elements.simulateButton.disabled = false;
}

async function analyze() {
  elements.analyzeButton.disabled = true;
  elements.analyzeButton.textContent = "Running analysis...";
  try {
    const response = await fetch("/betting/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(currentRequestBody()),
    });
    if (!response.ok) {
      const detail = await response.text();
      throw new Error(detail || "Failed to analyze match.");
    }
    const payload = await response.json();
    renderAnalysis(payload);
    state.forceRefresh = false;
  } finally {
    elements.analyzeButton.disabled = false;
    elements.analyzeButton.textContent = "Calculate Probabilities";
  }
}

function syncTeamSelectors() {
  if (!state.options) return;
  const allTeams = state.options.team_options || [];
  renderSelect(elements.homeTeam, allTeams, elements.homeTeam.value || allTeams[0]?.value || "");
  const awayTeams = allTeams.filter((team) => team.value !== elements.homeTeam.value);
  const desiredAway = awayTeams.some((team) => team.value === elements.awayTeam.value)
    ? elements.awayTeam.value
    : awayTeams[0]?.value || "";
  renderSelect(elements.awayTeam, awayTeams, desiredAway);
}

async function refreshOptionsFromControls() {
  await loadOptions();
  syncTeamSelectors();
}

function wireEvents() {
  elements.trainPct.addEventListener("input", () => {
    elements.trainPctLabel.textContent = `${Math.round(Number(elements.trainPct.value) * 100)}%`;
  });
  elements.league.addEventListener("change", refreshOptionsFromControls);
  elements.season.addEventListener("change", refreshOptionsFromControls);
  elements.homeTeam.addEventListener("change", syncTeamSelectors);
  elements.analyzeButton.addEventListener("click", analyze);
  elements.simulateButton.addEventListener("click", () => {
    if (!state.analysis) return;
    elements.simulatedResult.textContent = `${state.analysis.home_team} ${state.analysis.simulated_result.home} - ${state.analysis.simulated_result.away} ${state.analysis.away_team}`;
  });
  elements.refreshButton.addEventListener("click", async () => {
    state.forceRefresh = true;
    await analyze();
  });
}

async function bootstrap() {
  renderSelect(
    elements.model,
    MODELS.map((model) => ({ value: model, label: model })),
    "Maher",
  );
  elements.trainPctLabel.textContent = `${Math.round(Number(elements.trainPct.value) * 100)}%`;
  await loadOptions();
  wireEvents();
}

bootstrap().catch((error) => {
  elements.sourceSummary.textContent = error.message;
});

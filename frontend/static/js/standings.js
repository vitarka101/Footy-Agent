const state = {
  standings: null,
  refreshing: false,
};

const REFRESH_STORAGE_KEY = "footy_agent_refresh_job";

const elements = {
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
  sharedRefreshStatus: document.getElementById("shared-refresh-status"),
};

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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

function renderPulse(pulse) {
  elements.standingsPulseTitle.textContent = pulse?.title || "League pulse";
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
  state.standings = payload;
  renderPulse(payload.pulse);
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
          <td>
            <div class="form-strip">${row.last5.map(resultBadge).join("")}</div>
          </td>
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

function setRefreshState(isRefreshing, message) {
  if (!elements.refreshDataButton || !elements.refreshStatusBadge) return;
  state.refreshing = isRefreshing;
  elements.refreshDataButton.disabled = isRefreshing;
  elements.refreshDataButton.textContent = isRefreshing ? "Refreshing..." : "Refresh Data";
  elements.refreshStatusBadge.textContent = message;
}

function wait(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function setSharedRefreshStatus(message) {
  if (elements.sharedRefreshStatus) {
    elements.sharedRefreshStatus.textContent = message;
  }
}

function clearRefreshJob() {
  window.localStorage.removeItem(REFRESH_STORAGE_KEY);
}

async function trackSharedRefreshStatus() {
  const jobId = window.localStorage.getItem(REFRESH_STORAGE_KEY);
  if (!jobId) {
    setSharedRefreshStatus("");
    return;
  }

  while (true) {
    const response = await fetch(`/refresh/${jobId}`);
    if (!response.ok) {
      clearRefreshJob();
      setSharedRefreshStatus("");
      return;
    }
    const payload = await response.json();
    if (payload.status === "queued" || payload.status === "running") {
      setSharedRefreshStatus("Refreshing in progress...");
      await wait(1500);
      continue;
    }
    clearRefreshJob();
    setSharedRefreshStatus(payload.status === "succeeded" ? "Refresh done." : "");
    return;
  }
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
  if (!elements.refreshDataButton || !elements.refreshStatusBadge) return;
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
    await pollRefreshJob(payload.job_id);
    try {
      await loadStandings(state.standings?.selected_country || "", state.standings?.selected_league || "");
    } catch (error) {
      // Keep the existing standings visible if the refreshed reload fails.
    }
  } catch (error) {
    // Keep refresh failures silent and continue showing the previously loaded data.
  } finally {
    setRefreshState(false, "");
  }
}

async function handleStandingsCountryChange() {
  await loadStandings(elements.standingsCountry.value, "");
}

async function handleStandingsLeagueChange() {
  await loadStandings(elements.standingsCountry.value, elements.standingsLeague.value);
}

async function boot() {
  elements.standingsCountry.addEventListener("change", handleStandingsCountryChange);
  elements.standingsLeague.addEventListener("change", handleStandingsLeagueChange);
  if (elements.refreshDataButton) {
    elements.refreshDataButton.addEventListener("click", refreshData);
  }

  try {
    await loadStandings();
  } catch (error) {
    elements.standingsTable.innerHTML = `<div class="standings-empty">${escapeHtml(error.message)}</div>`;
  }
  trackSharedRefreshStatus().catch(() => setSharedRefreshStatus(""));
}

boot();

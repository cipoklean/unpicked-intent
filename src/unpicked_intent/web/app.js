"use strict";

/* Unpicked Intent — Silence Report & Draft Replay Frontend
 * Plain offline JS talking strictly to FastAPI endpoints:
 *   GET /meta
 *   GET /teams
 *   GET /silence/{team_id}
 *   GET /self-scout
 *   GET /replay/{team_id}/games
 *   GET /replay/{team_id}/{game_id}
 */

const TARGET_TEAMS_ORDER = [
  "340",
  "47363",
  "54198",
  "53073",
  "52416",
  "47499",
  "56389",
  "47351",
];
const TARGET_TEAM_IDS = new Set(TARGET_TEAMS_ORDER);
const SELF_SCOUT_TEAM_ID = "47351";

const ROLE_ORDER = ["top", "jungle", "mid", "bot", "support"];
const ROLE_LABELS = {
  top: "Top",
  jungle: "Jungle",
  mid: "Mid",
  bot: "Bot",
  support: "Support",
};
const ROLE_SHORT = {
  top: "Top",
  jungle: "Jgl",
  mid: "Mid",
  bot: "Bot",
  support: "Sup",
};

const PHASE_ORDER = ["ban1", "pick1", "ban2", "pick2"];
const PHASE_LABELS = {
  ban1: "Ban phase 1 · slots 1–6",
  pick1: "Pick phase 1 · slots 7–12",
  ban2: "Ban phase 2 · slots 13–16",
  pick2: "Pick phase 2 · slots 17–20",
};
const PHASE_SHORT = {
  ban1: "Ban 1",
  pick1: "Pick 1",
  ban2: "Ban 2",
  pick2: "Pick 2",
};

const state = {
  teams: [],
  totalTests: null,
  teamQuery: "",
  activeTeamId: null,
  lastOpponentTeamId: null,
  selfScoutMode: false,
  activeView: "silence", // 'silence' | 'replay'
  report: null,
  requestSeq: 0,
  // Left-open filters
  roleFilter: "all",
  playerFilter: "all",
  tierFilter: "all",
  groupBy: "flat", // 'flat' | 'role' | 'player'
  // Early-ban filters
  earlyOnly: false,
  banQuery: "",
  // Draft Replay state
  replayTeamId: null,
  replayGames: [],
  replayGameId: null,
  replayDetail: null,
  replaySlotIdx: 0,
  replayFilter: "all", // 'all' | 'open' | 'resolved'
  replayReqSeq: 0,
};

const els = {
  dbStatusPill: document.getElementById("db-status-pill"),
  selfScoutToggle: document.getElementById("self-scout-toggle"),
  selfScoutBanner: document.getElementById("self-scout-banner"),
  teamCountBadge: document.getElementById("team-count-badge"),
  teamSearch: document.getElementById("team-search"),
  teamList: document.getElementById("team-list"),
  statePanel: document.getElementById("state-panel"),
  reportContent: document.getElementById("report-content"),
  reportModeEyebrow: document.getElementById("report-mode-eyebrow"),
  reportTeamName: document.getElementById("report-team-name"),
  kpiStrongChip: document.getElementById("kpi-strong-chip"),
  kpiStrongCount: document.getElementById("kpi-strong-count"),
  kpiStrongSub: document.getElementById("kpi-strong-sub"),
  kpiLeadCount: document.getElementById("kpi-lead-count"),
  kpiEarlyBanCount: document.getElementById("kpi-early-ban-count"),
  kpiPatternsTestedCount: document.getElementById("kpi-patterns-tested-count"),
  viewTabs: document.getElementById("view-tabs"),
  tabSilence: document.getElementById("tab-silence"),
  tabReplay: document.getElementById("tab-replay"),
  silenceView: document.getElementById("silence-view"),
  replayView: document.getElementById("replay-view"),
  reportMinNNote: document.getElementById("report-min-n-note"),
  actionsHeading: document.getElementById("actions-heading"),
  actionsSubtitle: document.getElementById("actions-subtitle"),
  actionsList: document.getElementById("actions-list"),
  tellsShowingCount: document.getElementById("tells-showing-count"),
  rolePills: document.getElementById("role-pills"),
  playerFilter: document.getElementById("player-filter"),
  tierPills: document.getElementById("tier-pills"),
  groupPills: document.getElementById("group-pills"),
  tellsContainer: document.getElementById("tells-container"),
  earlyOnlyToggle: document.getElementById("early-only-toggle"),
  banSearch: document.getElementById("ban-search"),
  earlyBanContainer: document.getElementById("early-ban-container"),
  // Draft Replay elements
  replayMinNNote: document.getElementById("replay-min-n-note"),
  replayGamesCount: document.getElementById("replay-games-count"),
  replayGamePicker: document.getElementById("replay-game-picker"),
  replayStatus: document.getElementById("replay-status"),
  replayBody: document.getElementById("replay-body"),
  replayGameMeta: document.getElementById("replay-game-meta"),
  replayPrevBtn: document.getElementById("replay-prev-btn"),
  replayNextBtn: document.getElementById("replay-next-btn"),
  replayNextOpenTellBtn: document.getElementById("replay-next-open-tell-btn"),
  replaySlotCounter: document.getElementById("replay-slot-counter"),
  replaySlotBanner: document.getElementById("replay-slot-banner"),
  replayBurnedBar: document.getElementById("replay-burned-bar"),
  replayDraftStrip: document.getElementById("replay-draft-strip"),
  replayTellsSub: document.getElementById("replay-tells-sub"),
  replayStateSummary: document.getElementById("replay-state-summary"),
  replayStatePills: document.getElementById("replay-state-pills"),
  replayTellsContainer: document.getElementById("replay-tells-container"),
};

/* ---------- Formatting & Escaping ---------- */

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatExpected(val) {
  const num = Number(val);
  if (!Number.isFinite(num)) return "0.0";
  return num.toFixed(1);
}

function formatSlot(val) {
  const num = Number(val);
  if (!Number.isFinite(num)) return "—";
  return num.toFixed(1);
}

function formatProb(val) {
  const num = Number(val);
  if (!Number.isFinite(num)) return "—";
  if (num < 0.0001) return "<0.0001";
  if (num < 0.01) return num.toFixed(4);
  if (num < 0.1) return num.toFixed(3);
  return num.toFixed(2);
}

function roleBadgeHtml(role) {
  const key = String(role || "").toLowerCase();
  const shortLabel = ROLE_SHORT[key] || escapeHtml(key.toUpperCase() || "—");
  const cls = ROLE_SHORT[key] ? `role-${key}` : "";
  return `<span class="role-tag ${cls}">${shortLabel}</span>`;
}

function signalBadgeHtml(label) {
  if (label === "strong") {
    return `<span class="badge-signal badge-strong"><span class="signal-dot"></span>STRONG</span>`;
  }
  return `<span class="badge-signal badge-lead"><span class="signal-dot"></span>LEAD</span>`;
}

/* ---------- API / Static Data Helper ---------- */

const IS_STATIC = document.documentElement.dataset.static === "1";

function resolveDataUrl(endpoint) {
  if (!IS_STATIC) return endpoint;
  if (endpoint === "/meta") return "data/meta.json";
  if (endpoint === "/teams") return "data/teams.json";
  if (endpoint === "/self-scout") return "data/self-scout.json";
  if (endpoint.startsWith("/silence/")) {
    const teamId = endpoint.slice("/silence/".length);
    return `data/silence/${teamId}.json`;
  }
  if (endpoint.startsWith("/replay/")) {
    const rest = endpoint.slice("/replay/".length);
    return `data/replay/${rest}.json`;
  }
  return endpoint;
}

async function fetchJson(endpoint) {
  const url = resolveDataUrl(endpoint);
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
  });
  let body = null;
  let parseError = false;
  try {
    body = await response.json();
  } catch {
    body = null;
    parseError = true;
  }
  if (!response.ok) {
    const fallbackDetail =
      response.status === 404
        ? "No exported report found for this team."
        : `Request failed with status ${response.status}`;
    const err = new Error((body && body.detail) || fallbackDetail);
    err.status = response.status;
    err.detail = (body && body.detail) || err.message;
    throw err;
  }
  if (parseError) {
    const err = new Error("Expected JSON response.");
    err.status =
      IS_STATIC && (endpoint.startsWith("/silence/") || endpoint.startsWith("/replay/"))
        ? 404
        : 500;
    err.detail =
      err.status === 404
        ? "No exported report found for this team."
        : "Received non-JSON response.";
    throw err;
  }
  return body;
}

/* ---------- State Panels (Loading / 503 / 404 / Error) ---------- */

function showStatePanel(html, variantClass = "") {
  els.reportContent.classList.add("hidden");
  els.statePanel.className = `state-panel ${variantClass}`.trim();
  els.statePanel.innerHTML = html;
  els.statePanel.classList.remove("hidden");
}

function showReportContent() {
  els.statePanel.classList.add("hidden");
  els.reportContent.classList.remove("hidden");
}

function renderLoadingState(message) {
  showStatePanel(
    `<div class="state-code">LOADING</div>
     <h2 class="state-title">${escapeHtml(message)}</h2>
     <p class="state-detail">Reading precomputed aggregates from derived/unpicked.duckdb…</p>`
  );
}

function render503State(detail) {
  els.dbStatusPill.textContent = "503 · DB missing";
  els.dbStatusPill.className = "status-pill is-error";
  showStatePanel(
    `<div class="state-code">HTTP 503 · SCOUTING DATABASE UNAVAILABLE</div>
     <h2 class="state-title">Build derived/unpicked.duckdb before opening the UI</h2>
     <p class="state-detail">${escapeHtml(detail)}</p>
     <pre class="code-block">uv run python -m unpicked_intent.pipeline.run_all --grid &lt;path-to-grid-repo&gt;</pre>
     <div class="state-actions">
       <button type="button" class="btn btn-primary" data-action="retry-init">Retry connection</button>
     </div>`,
    "state-503"
  );
}

function render404State(teamId, detail) {
  showStatePanel(
    `<div class="state-code">HTTP 404 · UNKNOWN TEAM</div>
     <h2 class="state-title">Team "${escapeHtml(teamId)}" was not found</h2>
     <p class="state-detail">${escapeHtml(detail)}</p>
     <div class="state-actions">
       <button type="button" class="btn btn-primary" data-action="select-first-team">Open first target team</button>
       <button type="button" class="btn" data-action="open-self-scout">Open Self-scout: Cloud9 Kia</button>
     </div>`,
    "state-404"
  );
}

function renderGenericError(detail) {
  showStatePanel(
    `<div class="state-code">REQUEST ERROR</div>
     <h2 class="state-title">Could not load scouting data</h2>
     <p class="state-detail">${escapeHtml(detail)}</p>
     <div class="state-actions">
       <button type="button" class="btn btn-primary" data-action="retry-init">Retry</button>
     </div>`
  );
}

/* ---------- Team Picker ---------- */

function renderTeamList() {
  const q = state.teamQuery.trim().toLowerCase();
  const filtered = state.teams.filter((t) => {
    if (!q) return true;
    return (
      String(t.name).toLowerCase().includes(q) ||
      String(t.team_id).toLowerCase().includes(q)
    );
  });

  els.teamCountBadge.textContent =
    filtered.length === state.teams.length
      ? `${state.teams.length}`
      : `${filtered.length}/${state.teams.length}`;

  if (filtered.length === 0) {
    els.teamList.innerHTML = `<div class="rail-placeholder">No teams match "${escapeHtml(state.teamQuery)}".</div>`;
    return;
  }

  // Server already returns TARGET_TEAMS first, then all others.
  const targetTeams = filtered.filter((t) => TARGET_TEAM_IDS.has(String(t.team_id)));
  const otherTeams = filtered.filter((t) => !TARGET_TEAM_IDS.has(String(t.team_id)));

  const parts = [];
  if (targetTeams.length > 0) {
    parts.push(`<div class="rail-section-label">LCS targets (${targetTeams.length})</div>`);
    parts.push(...targetTeams.map(renderTeamButtonHtml));
  }
  if (otherTeams.length > 0) {
    parts.push(`<div class="rail-section-label">All other teams (${otherTeams.length})</div>`);
    parts.push(...otherTeams.map(renderTeamButtonHtml));
  }
  els.teamList.innerHTML = parts.join("");
}

function renderTeamButtonHtml(team) {
  const tid = String(team.team_id);
  const isActive =
    (state.selfScoutMode && tid === SELF_SCOUT_TEAM_ID) ||
    (!state.selfScoutMode && state.activeTeamId === tid);
  const c9Tag = tid === SELF_SCOUT_TEAM_ID ? `<span class="c9-tag">Self</span>` : "";

  return `<button
    type="button"
    class="team-item ${isActive ? "active" : ""}"
    data-team-id="${escapeHtml(tid)}"
    aria-current="${isActive ? "true" : "false"}"
    title="${escapeHtml(team.name)} — ${team.left_open_count} left-open tells, ${team.early_ban_count} early-ban rows"
  >
    <span class="team-item-name">
      <span class="team-item-text">${escapeHtml(team.name)}</span>
      ${c9Tag}
    </span>
    <span class="team-counts">
      <span class="count-tells">${Number(team.left_open_count)}</span>
      <span class="count-sep">/</span>
      <span class="count-bans">${Number(team.early_ban_count)}</span>
    </span>
  </button>`;
}

/* ---------- Recommended Actions (strictly from API fields) ---------- */

const ACTION_LEAD_MAX_P = 0.05;

function isQualifyingActionLead(tell) {
  if (tell.label === "strong") return false;
  if (tell.p_value === null || tell.p_value === undefined || tell.p_value === "") {
    return false;
  }
  const p = Number(tell.p_value);
  return Number.isFinite(p) && p < ACTION_LEAD_MAX_P;
}

function buildRecommendedActions(report, isSelfScout) {
  const actions = [];
  const tells = Array.isArray(report.left_open) ? report.left_open : [];
  const bans = Array.isArray(report.early_ban) ? report.early_ban : [];

  // Prioritize strong tells first, then leads with p_value < 0.05 (up to 3 tells).
  // If no leads qualify, show only strong tells and early bans.
  const strongTells = tells.filter((t) => t.label === "strong");
  const leadTells = tells.filter(isQualifyingActionLead);
  const topTells = [...strongTells, ...leadTells].slice(0, 3);

  for (const tell of topTells) {
    const exp = formatExpected(tell.expected);
    const taken = Number(tell.taken);
    const k = Number(tell.k);
    const underExpected = taken < Number(tell.expected);
    const isStrong = tell.label === "strong";

    let line;
    if (isSelfScout) {
      if (underExpected) {
        const tail = isStrong
          ? "They may leave it open."
          : "So far, leaving it open has rarely drawn it.";
        line = `Opponents see ${tell.player_name} took ${tell.champion} in ${taken} of ${k} games where it was open (about ${exp} expected). ${tail}`;
      } else {
        line = `Opponents see ${tell.player_name} took ${tell.champion} in ${taken} of ${k} games where it was open (about ${exp} expected).`;
      }
    } else {
      if (underExpected) {
        const tail = isStrong
          ? "Leaving it open rarely draws it."
          : "So far, leaving it open has rarely drawn it.";
        line = `${tell.player_name} has taken ${tell.champion} in ${taken} of ${k} games where it was open (about ${exp} expected). ${tail}`;
      } else {
        line = `${tell.player_name} has taken ${tell.champion} in ${taken} of ${k} games where it was open (about ${exp} expected).`;
      }
    }

    actions.push({
      kind: isStrong ? "strong" : "lead",
      text: line,
    });
  }

  // Top positive-delta early bans (up to 2). Always include n_bans.
  const topEarlyBans = bans.filter((b) => Number(b.delta) > 0).slice(0, 2);
  for (const ban of topEarlyBans) {
    const teamSlot = formatSlot(ban.team_mean_slot);
    const baseSlot = formatSlot(ban.baseline_mean_slot);
    const nBans = Number(ban.n_bans);
    const line = isSelfScout
      ? `We ban ${ban.champion} at slot ${teamSlot} on average vs ${baseSlot} normally (${nBans} bans). Opponents expect it gone early.`
      : `They ban ${ban.champion} at slot ${teamSlot} on average vs ${baseSlot} normally (${nBans} bans). Expect it gone early.`;

    actions.push({
      kind: "ban",
      text: line,
    });
  }

  return actions;
}

function renderRecommendedActions(report) {
  if (state.selfScoutMode) {
    els.actionsHeading.textContent = "Exposure Brief — What Opponents Read";
    els.actionsSubtitle.textContent =
      "Lines opposing coaches can read directly from Cloud9 Kia's ranked tells and early-ban slots.";
  } else {
    els.actionsHeading.textContent = "Recommended Actions";
    els.actionsSubtitle.textContent =
      "Draft prep takeaways written strictly from ranked tells and early-ban slot averages.";
  }

  const actions = buildRecommendedActions(report, state.selfScoutMode);
  if (actions.length === 0) {
    els.actionsList.innerHTML = `<li class="action-empty">No qualifying tells or earlier-than-meta bans (n &ge; ${Number(report.min_n || 4)}) available to summarize for this team.</li>`;
    return;
  }

  els.actionsList.innerHTML = actions
    .map((item) => {
      let badge;
      let rowCls;
      if (item.kind === "strong") {
        badge = signalBadgeHtml("strong");
        rowCls = "action-strong";
      } else if (item.kind === "lead") {
        badge = signalBadgeHtml("lead");
        rowCls = "action-lead";
      } else {
        badge = `<span class="badge-signal badge-ban">EARLY BAN</span>`;
        rowCls = "action-ban";
      }
      return `<li class="action-item ${rowCls}">
        ${badge}
        <span class="action-text">${escapeHtml(item.text)}</span>
      </li>`;
    })
    .join("");
}

/* ---------- Left-Open Tells Controls & Table ---------- */

function syncTellFiltersForReport(report) {
  const tells = Array.isArray(report.left_open) ? report.left_open : [];

  // Role counts
  const roleCounts = new Map();
  for (const r of ROLE_ORDER) roleCounts.set(r, 0);
  for (const t of tells) {
    const key = String(t.role || "").toLowerCase();
    roleCounts.set(key, (roleCounts.get(key) || 0) + 1);
  }

  // Render role pills
  const roleButtons = [
    `<button type="button" class="pill ${state.roleFilter === "all" ? "active" : ""}" data-role="all">All <span class="pill-count">${tells.length}</span></button>`,
  ];
  for (const r of ROLE_ORDER) {
    const count = roleCounts.get(r) || 0;
    roleButtons.push(
      `<button type="button" class="pill ${state.roleFilter === r ? "active" : ""}" data-role="${r}">${ROLE_LABELS[r]} <span class="pill-count">${count}</span></button>`
    );
  }
  els.rolePills.innerHTML = roleButtons.join("");

  // Populate player dropdown
  const players = Array.from(new Set(tells.map((t) => t.player_name))).sort((a, b) =>
    a.localeCompare(b)
  );
  if (state.playerFilter !== "all" && !players.includes(state.playerFilter)) {
    state.playerFilter = "all";
  }
  const playerOpts = [`<option value="all">All players (${players.length})</option>`];
  for (const name of players) {
    const selected = state.playerFilter === name ? " selected" : "";
    playerOpts.push(`<option value="${escapeHtml(name)}"${selected}>${escapeHtml(name)}</option>`);
  }
  els.playerFilter.innerHTML = playerOpts.join("");

  // Update tier counts
  const strongCount = tells.filter((t) => t.label === "strong").length;
  const leadCount = tells.length - strongCount;
  els.tierPills.querySelectorAll(".pill").forEach((btn) => {
    const tier = btn.dataset.tier;
    btn.classList.toggle("active", state.tierFilter === tier);
    if (tier === "all") btn.innerHTML = `All <span class="pill-count">${tells.length}</span>`;
    if (tier === "strong") btn.innerHTML = `Strong <span class="pill-count">${strongCount}</span>`;
    if (tier === "lead") btn.innerHTML = `Leads <span class="pill-count">${leadCount}</span>`;
  });

  els.groupPills.querySelectorAll(".pill").forEach((btn) => {
    btn.classList.toggle("active", state.groupBy === btn.dataset.group);
  });
}

function getFilteredTells(report) {
  const tells = Array.isArray(report.left_open) ? report.left_open : [];
  return tells.filter((t) => {
    if (state.roleFilter !== "all" && String(t.role).toLowerCase() !== state.roleFilter) {
      return false;
    }
    if (state.playerFilter !== "all" && t.player_name !== state.playerFilter) {
      return false;
    }
    if (state.tierFilter !== "all" && t.label !== state.tierFilter) {
      return false;
    }
    return true;
  });
}

function renderTellRowHtml(tell) {
  const isStrong = tell.label === "strong";
  const k = Number(tell.k);
  const taken = Number(tell.taken);
  const expected = Number(tell.expected);
  const obsPct = k > 0 ? Math.min(100, Math.max(0, (taken / k) * 100)) : 0;
  const expPct = k > 0 ? Math.min(100, Math.max(0, (expected / k) * 100)) : 0;

  return `<tr class="tell-row ${isStrong ? "row-strong" : "row-lead"}">
    <td>${signalBadgeHtml(tell.label)}</td>
    <td>
      <div class="champ-cell">
        ${roleBadgeHtml(tell.role)}
        <span class="champ-name">${escapeHtml(tell.champion)}</span>
      </div>
    </td>
    <td class="player-name">${escapeHtml(tell.player_name)}</td>
    <td class="evidence-cell">
      <div class="evidence-words">
        Taken in <strong>${taken}</strong> of <strong>${k}</strong> open games
        <span class="exp-muted">&middot; expected <strong>${formatExpected(expected)}</strong></span>
      </div>
      <div class="rate-bar-wrap" title="Observed ${taken}/${k} vs expected ${formatExpected(expected)}/${k}">
        <div class="rate-bar-obs" style="width: ${obsPct.toFixed(1)}%"></div>
        <div class="rate-bar-exp" style="left: ${expPct.toFixed(1)}%"></div>
      </div>
    </td>
    <td class="num p-val">${formatProb(tell.p_value)}</td>
    <td class="num ${isStrong ? "q-strong" : "q-lead"}">${formatProb(tell.q_value)}</td>
  </tr>`;
}

function renderTellsTableHtml(rows) {
  return `<div class="table-scroll">
    <table class="data-table">
      <thead>
        <tr>
          <th>Tier</th>
          <th>Champion</th>
          <th>Player</th>
          <th>Observed vs expected (n open games)</th>
          <th class="num">p</th>
          <th class="num">q</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map(renderTellRowHtml).join("")}
      </tbody>
    </table>
  </div>`;
}

function renderLeftOpenTells(report) {
  const allTells = Array.isArray(report.left_open) ? report.left_open : [];
  const filtered = getFilteredTells(report);

  els.tellsShowingCount.textContent =
    filtered.length === allTells.length
      ? `${allTells.length} tells`
      : `Showing ${filtered.length} of ${allTells.length}`;

  if (allTells.length === 0) {
    els.tellsContainer.innerHTML = `<div class="empty-table-msg">No left-open tells with at least ${Number(report.min_n || 4)} qualifying games for this team.</div>`;
    return;
  }

  if (filtered.length === 0) {
    els.tellsContainer.innerHTML = `<div class="empty-table-msg">No tells match the active role, player, or tier filters.</div>`;
    return;
  }

  if (state.groupBy === "flat") {
    els.tellsContainer.innerHTML = renderTellsTableHtml(filtered);
    return;
  }

  const groups = new Map();
  if (state.groupBy === "role") {
    for (const r of ROLE_ORDER) groups.set(r, []);
    for (const t of filtered) {
      const key = String(t.role || "unknown").toLowerCase();
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(t);
    }
  } else {
    for (const t of filtered) {
      const key = t.player_name;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(t);
    }
  }

  const blocks = [];
  for (const [key, items] of groups.entries()) {
    if (items.length === 0) continue;
    const strongInGroup = items.filter((i) => i.label === "strong").length;
    const leadsInGroup = items.length - strongInGroup;
    const headerLeft =
      state.groupBy === "role"
        ? `${roleBadgeHtml(key)}<span>${escapeHtml(ROLE_LABELS[key] || key)}</span>`
        : `${roleBadgeHtml(items[0].role)}<span>${escapeHtml(key)}</span>`;

    blocks.push(`<div class="group-block">
      <div class="group-header">
        <div class="group-header-left">${headerLeft}</div>
        <div class="group-header-stats">${items.length} tells &middot; ${strongInGroup} strong, ${leadsInGroup} leads</div>
      </div>
      ${renderTellsTableHtml(items)}
    </div>`);
  }

  els.tellsContainer.innerHTML = blocks.join("");
}

/* ---------- Early-Ban Pressure Table ---------- */

function slotToPct(slot) {
  const clamped = Math.min(20, Math.max(1, Number(slot) || 1));
  return ((clamped - 1) / 19) * 100;
}

function renderEarlyBanTable(report) {
  const allBans = Array.isArray(report.early_ban) ? report.early_ban : [];
  const q = state.banQuery.trim().toLowerCase();

  const filtered = allBans.filter((b) => {
    if (state.earlyOnly && Number(b.delta) <= 0) return false;
    if (q && !String(b.champion).toLowerCase().includes(q)) return false;
    return true;
  });

  if (allBans.length === 0) {
    els.earlyBanContainer.innerHTML = `<div class="empty-table-msg">No champions with at least 4 bans and a meta baseline for this team.</div>`;
    return;
  }

  if (filtered.length === 0) {
    els.earlyBanContainer.innerHTML = `<div class="empty-table-msg">No early-ban rows match the current filter.</div>`;
    return;
  }

  const rowsHtml = filtered
    .map((ban) => {
      const delta = Number(ban.delta);
      const isEarlier = delta > 0;
      const teamPct = slotToPct(ban.team_mean_slot);
      const basePct = slotToPct(ban.baseline_mean_slot);
      const leftPct = Math.min(teamPct, basePct);
      const widthPct = Math.max(1.5, Math.abs(teamPct - basePct));
      const deltaSign = delta > 0 ? "+" : "";
      const deltaDir = delta > 0 ? "earlier" : delta < 0 ? "later" : "same";
      const cls = isEarlier ? "is-earlier" : "is-later";

      return `<tr class="ban-row">
        <td><span class="champ-name">${escapeHtml(ban.champion)}</span></td>
        <td class="num">n = ${Number(ban.n_bans)}</td>
        <td class="num">${formatSlot(ban.team_mean_slot)}</td>
        <td class="num">${formatSlot(ban.baseline_mean_slot)}</td>
        <td class="slot-track-cell">
          <div class="slot-track" title="Team avg slot ${formatSlot(ban.team_mean_slot)} vs meta avg ${formatSlot(ban.baseline_mean_slot)} (${Number(ban.n_bans)} bans)">
            <div class="slot-zone-ban1"></div>
            <div class="slot-zone-ban2"></div>
            <div class="slot-span ${cls}" style="left: ${leftPct.toFixed(1)}%; width: ${widthPct.toFixed(1)}%"></div>
            <div class="slot-dot-base" style="left: ${basePct.toFixed(1)}%"></div>
            <div class="slot-dot-team ${cls}" style="left: ${teamPct.toFixed(1)}%"></div>
          </div>
        </td>
        <td class="num ${isEarlier ? "delta-pos" : "delta-neg"}">
          ${deltaSign}${delta.toFixed(1)} slots ${deltaDir}
        </td>
      </tr>`;
    })
    .join("");

  els.earlyBanContainer.innerHTML = `<table class="data-table">
    <thead>
      <tr>
        <th>Champion</th>
        <th class="num">Bans (n)</th>
        <th class="num">Team avg slot</th>
        <th class="num">Meta avg slot</th>
        <th>Slot shift (1 &rarr; 20)</th>
        <th class="num">Delta</th>
      </tr>
    </thead>
    <tbody>${rowsHtml}</tbody>
  </table>`;
}

/* ---------- Draft Replay View ---------- */

function isTargetTeam(teamId) {
  return TARGET_TEAM_IDS.has(String(teamId));
}

function updateUrlHash() {
  let targetHash;
  if (state.selfScoutMode) {
    if (state.activeView === "replay") {
      targetHash = state.replayGameId
        ? `#/self-scout/replay/${encodeURIComponent(state.replayGameId)}`
        : "#/self-scout/replay";
    } else {
      targetHash = "#/self-scout";
    }
  } else if (state.activeTeamId) {
    const tid = encodeURIComponent(state.activeTeamId);
    if (state.activeView === "replay" && isTargetTeam(state.activeTeamId)) {
      targetHash = state.replayGameId
        ? `#/team/${tid}/replay/${encodeURIComponent(state.replayGameId)}`
        : `#/team/${tid}/replay`;
    } else {
      targetHash = `#/team/${tid}`;
    }
  } else {
    return;
  }
  if (window.location.hash !== targetHash) {
    window.history.replaceState(null, "", targetHash);
  }
}

function syncActiveViewTabs() {
  const canReplay = isTargetTeam(state.activeTeamId);
  els.tabReplay.classList.toggle("hidden", !canReplay);

  if (!canReplay && state.activeView === "replay") {
    state.activeView = "silence";
  }

  const isReplay = state.activeView === "replay" && canReplay;
  els.tabSilence.classList.toggle("active", !isReplay);
  els.tabSilence.setAttribute("aria-selected", isReplay ? "false" : "true");
  els.tabReplay.classList.toggle("active", isReplay);
  els.tabReplay.setAttribute("aria-selected", isReplay ? "true" : "false");

  els.silenceView.classList.toggle("hidden", isReplay);
  els.replayView.classList.toggle("hidden", !isReplay);
}

function showReplayStatus(html) {
  els.replayBody.classList.add("hidden");
  els.replayStatus.innerHTML = html;
  els.replayStatus.classList.remove("hidden");
}

function showReplayBody() {
  els.replayStatus.classList.add("hidden");
  els.replayBody.classList.remove("hidden");
}

function renderReplayGamePicker() {
  const games = state.replayGames;
  els.replayGamesCount.textContent =
    games.length === 1 ? "1 game" : `${games.length} games`;

  if (games.length === 0) {
    els.replayGamePicker.innerHTML = `<div class="empty-table-msg">No complete games found for this team.</div>`;
    return;
  }

  els.replayGamePicker.innerHTML = games
    .map((g) => {
      const isActive = g.game_id === state.replayGameId;
      const sideKey = String(g.side || "").toLowerCase();
      const sideCls = sideKey === "blue" ? "side-blue" : "side-red";
      const sideLabel = sideKey === "blue" ? "Blue" : "Red";
      const fearlessCls = Number(g.game_num) >= 2 ? "is-fearless" : "";

      return `<button
        type="button"
        class="game-card ${sideCls} ${isActive ? "active" : ""}"
        data-game-id="${escapeHtml(g.game_id)}"
        role="option"
        aria-selected="${isActive ? "true" : "false"}"
      >
        <div class="game-card-top">
          <span class="game-card-opp">vs ${escapeHtml(g.opponent_name || g.opponent_id)}</span>
          <span class="game-num-tag ${fearlessCls}">G${Number(g.game_num)}</span>
        </div>
        <div class="game-card-meta">
          <span class="side-pill ${sideCls}">${sideLabel}</span>
          <span>${escapeHtml(g.played_at || "—")}</span>
          <span>Patch ${escapeHtml(g.patch || "—")}</span>
        </div>
      </button>`;
    })
    .join("");
}

function findNextOpenTellSlotIdx(detail, fromIdx) {
  if (!detail) return -1;
  const slots = Array.isArray(detail.slots) ? detail.slots : [];
  const tells = Array.isArray(detail.tells) ? detail.tells : [];
  if (slots.length === 0 || tells.length === 0) return -1;

  const targetTeamId = String(detail.team_id);
  const start = Math.max(0, Number(fromIdx) + 1);
  for (let idx = start; idx < slots.length; idx += 1) {
    const slot = slots[idx];
    if (!slot || String(slot.team_id) !== targetTeamId) continue;
    const hasOpenTell = tells.some(
      (tell) => Array.isArray(tell.slot_states) && tell.slot_states[idx] === "open"
    );
    if (hasOpenTell) return idx;
  }
  return -1;
}

function advanceToNextOpenTellSlot() {
  if (!state.replayDetail) return false;
  const nextIdx = findNextOpenTellSlotIdx(state.replayDetail, state.replaySlotIdx);
  if (nextIdx < 0) return false;
  state.replaySlotIdx = nextIdx;
  renderReplaySlotView();
  return true;
}

function renderReplayDraftStrip(detail) {
  const slots = Array.isArray(detail.slots) ? detail.slots : [];
  const currentIdx = Math.min(Math.max(0, state.replaySlotIdx), Math.max(0, slots.length - 1));
  const currentSlot = slots[currentIdx];

  // Determine Blue and Red team labels from slots
  const blueSlot = slots.find((s) => s.side === "blue");
  const redSlot = slots.find((s) => s.side === "red");
  const blueTeamName = blueSlot ? blueSlot.team_name : "Blue side";
  const redTeamName = redSlot ? redSlot.team_name : "Red side";
  const blueIsScouted = blueSlot && String(blueSlot.team_id) === String(detail.team_id);
  const redIsScouted = redSlot && String(redSlot.team_id) === String(detail.team_id);

  const headerHtml = `<div class="draft-sides-header">
    <div class="draft-side-title side-blue">
      <span>Blue · ${escapeHtml(blueTeamName)}</span>
      ${blueIsScouted ? `<span class="scouted-tag">Scouted</span>` : ""}
    </div>
    <div class="draft-side-title side-red">
      <span>Red · ${escapeHtml(redTeamName)}</span>
      ${redIsScouted ? `<span class="scouted-tag">Scouted</span>` : ""}
    </div>
  </div>`;

  const phasesHtml = PHASE_ORDER.map((phase) => {
    const phaseSlots = slots
      .map((s, idx) => ({ slotObj: s, idx }))
      .filter(({ slotObj }) => slotObj.phase === phase);

    const blueCells = phaseSlots.filter(({ slotObj }) => slotObj.side === "blue");
    const redCells = phaseSlots.filter(({ slotObj }) => slotObj.side === "red");

    const renderCell = ({ slotObj, idx }) => {
      const isCurrent = idx === currentIdx;
      const isFuture = idx > currentIdx;
      const isBan = slotObj.action === "ban";
      const sideCls = slotObj.side === "blue" ? "side-blue" : "side-red";
      const actionCls = isBan ? "action-ban" : "action-pick";
      const stateCls = isCurrent ? "is-current" : isFuture ? "is-future" : "is-past";
      const actionTag = isBan
        ? `<span class="draft-action-tag tag-ban">Ban</span>`
        : `<span class="draft-action-tag tag-pick">Pick</span>`;

      return `<button
        type="button"
        class="draft-cell ${sideCls} ${actionCls} ${stateCls}"
        data-slot-idx="${idx}"
        aria-current="${isCurrent ? "step" : "false"}"
        title="Slot ${slotObj.slot}: ${escapeHtml(slotObj.team_name)} ${isBan ? "banned" : "picked"} ${escapeHtml(slotObj.champion)}"
      >
        <span class="draft-cell-head">
          <span class="draft-slot-num">#${Number(slotObj.slot)}</span>
          ${actionTag}
        </span>
        <span class="champ-chip">${escapeHtml(slotObj.champion)}</span>
      </button>`;
    };

    return `<div class="draft-phase-block">
      <div class="draft-phase-label">${PHASE_LABELS[phase]}</div>
      <div class="draft-phase-columns">
        <div class="draft-side-row">${blueCells.map(renderCell).join("")}</div>
        <div class="draft-side-row">${redCells.map(renderCell).join("")}</div>
      </div>
    </div>`;
  }).join("");

  els.replayDraftStrip.innerHTML = headerHtml + phasesHtml;

  // Update stepper controls & current slot banner
  els.replayPrevBtn.disabled = currentIdx <= 0;
  els.replayNextBtn.disabled = currentIdx >= slots.length - 1;
  const nextOpenTellIdx = findNextOpenTellSlotIdx(detail, currentIdx);
  els.replayNextOpenTellBtn.disabled = nextOpenTellIdx < 0;
  els.replaySlotCounter.textContent =
    slots.length > 0 ? `Slot ${currentSlot.slot} / ${slots.length}` : "No slots";

  if (currentSlot) {
    const isTargetActing = String(currentSlot.team_id) === String(detail.team_id);
    const sideCls = currentSlot.side === "blue" ? "side-blue" : "side-red";
    const sideLabel = currentSlot.side === "blue" ? "Blue" : "Red";
    const phaseLabel = PHASE_SHORT[currentSlot.phase] || currentSlot.phase;
    const verb = currentSlot.action === "ban" ? "banned" : "picked";
    const actingPill = isTargetActing
      ? `<span class="acting-pill">${escapeHtml(detail.team_name)} acting</span>`
      : "";

    els.replaySlotBanner.className = `replay-slot-banner ${isTargetActing ? "is-target-acting" : ""}`.trim();
    els.replaySlotBanner.innerHTML = `
      <div class="slot-banner-main">
        <span class="side-pill ${sideCls}">${sideLabel}</span>
        <span class="game-num-tag">${escapeHtml(phaseLabel)}</span>
        <span class="slot-banner-text">
          <strong>${escapeHtml(currentSlot.team_name)}</strong> ${verb}
          <strong>${escapeHtml(currentSlot.champion)}</strong>
        </span>
        ${actingPill}
      </div>
      <span class="slot-kbd-hint">&larr; / &rarr; step &middot; n next open tell</span>
    `;
  } else {
    els.replaySlotBanner.className = "replay-slot-banner";
    els.replaySlotBanner.innerHTML = "";
  }
}

function renderReplayBurnedBar(detail) {
  const burned = Array.isArray(detail.burned) ? detail.burned : [];
  if (burned.length === 0) {
    els.replayBurnedBar.classList.add("hidden");
    els.replayBurnedBar.innerHTML = "";
    return;
  }

  const tellChamps = new Set((detail.tells || []).map((t) => t.champion));
  const chips = burned
    .map((champ) => {
      const cls = tellChamps.has(champ) ? "burned-chip is-tell-champ" : "burned-chip";
      return `<span class="${cls}">${escapeHtml(champ)}</span>`;
    })
    .join("");

  els.replayBurnedBar.innerHTML = `
    <span class="burned-bar-label">Fearless burned (${burned.length} from earlier games):</span>
    ${chips}
  `;
  els.replayBurnedBar.classList.remove("hidden");
}

function renderReplayTellsPanel(detail) {
  const slots = Array.isArray(detail.slots) ? detail.slots : [];
  const tells = Array.isArray(detail.tells) ? detail.tells : [];
  const currentIdx = Math.min(Math.max(0, state.replaySlotIdx), Math.max(0, slots.length - 1));
  const currentSlot = slots[currentIdx];
  const isTargetActing = Boolean(
    currentSlot && String(currentSlot.team_id) === String(detail.team_id)
  );
  const opponentName = (detail.game && detail.game.opponent_name) || "opponent";

  els.replayTellsSub.textContent = currentSlot
    ? isTargetActing
      ? `Slot ${currentSlot.slot} (${detail.team_name} acting): open tells are highlighted below as skipped opportunities.`
      : `Slot ${currentSlot.slot} (${currentSlot.team_name} acting): live availability of ${detail.team_name}'s surfaced tells (n ≥ ${Number(detail.min_n || 4)}).`
    : "Shows whether each surfaced tell is open, taken, denied, banned, or burned at this draft slot.";

  // Update state filter pills active state
  els.replayStatePills.querySelectorAll(".pill").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.replayFilter === state.replayFilter);
  });

  if (tells.length === 0) {
    els.replayStateSummary.textContent = "0 tells";
    els.replayTellsContainer.innerHTML = `<div class="empty-table-msg">No surfaced tells with at least ${Number(detail.min_n || 4)} qualifying games for this team.</div>`;
    return;
  }

  const counts = {
    open: 0,
    picked_by_team: 0,
    picked_by_opponent: 0,
    banned: 0,
    burned: 0,
  };

  const enriched = tells.map((tell, origIdx) => {
    const states = Array.isArray(tell.slot_states) ? tell.slot_states : [];
    const curState = states[currentIdx] || "open";
    const prevState = currentIdx > 0 ? states[currentIdx - 1] || "open" : "open";
    const changedHere = curState !== "burned" && curState !== "open" && curState !== prevState;
    if (Object.prototype.hasOwnProperty.call(counts, curState)) {
      counts[curState] += 1;
    }
    return { tell, curState, changedHere, origIdx };
  });

  const summaryParts = [`${counts.open} open`];
  if (counts.picked_by_team > 0) summaryParts.push(`${counts.picked_by_team} taken`);
  if (counts.picked_by_opponent > 0) summaryParts.push(`${counts.picked_by_opponent} denied`);
  if (counts.banned > 0) summaryParts.push(`${counts.banned} banned`);
  if (counts.burned > 0) summaryParts.push(`${counts.burned} burned`);
  els.replayStateSummary.textContent = summaryParts.join(" · ");

  const filtered = enriched.filter(({ curState }) => {
    if (state.replayFilter === "open") return curState === "open";
    if (state.replayFilter === "resolved") return curState !== "open";
    return true;
  });

  if (filtered.length === 0) {
    els.replayTellsContainer.innerHTML = `<div class="empty-table-msg">No tells match this state filter at slot ${currentSlot ? currentSlot.slot : 1}.</div>`;
    return;
  }

  // Surface newly-resolved tells on this slot first, then open tells, then unavailable tells,
  // preserving p_value rank (origIdx) within each group.
  const bucketOf = (item) => {
    if (item.changedHere) return 0;
    if (item.curState === "open") return 1;
    return 2;
  };
  filtered.sort((a, b) => bucketOf(a) - bucketOf(b) || a.origIdx - b.origIdx);

  const rowsHtml = filtered
    .map(({ tell, curState, changedHere }) => {
      const isStrong = tell.label === "strong";
      const k = Number(tell.k);
      const taken = Number(tell.taken);
      const expected = Number(tell.expected);
      const obsPct = k > 0 ? Math.min(100, Math.max(0, (taken / k) * 100)) : 0;
      const expPct = k > 0 ? Math.min(100, Math.max(0, (expected / k) * 100)) : 0;
      const isOpenActing = curState === "open" && isTargetActing;

      let stateHtml;
      if (isOpenActing) {
        stateHtml = `<span class="state-badge state-open-acting">Open now, history says they skip it</span>`;
      } else if (curState === "open") {
        stateHtml = `<span class="state-badge state-open">Open</span>`;
      } else if (curState === "picked_by_team") {
        stateHtml = `<span class="state-badge state-taken">Picked by ${escapeHtml(detail.team_name)}</span>`;
      } else if (curState === "picked_by_opponent") {
        stateHtml = `<span class="state-badge state-denied">Denied by ${escapeHtml(opponentName)}</span>`;
      } else if (curState === "banned") {
        stateHtml = `<span class="state-badge state-banned">Banned</span>`;
      } else {
        stateHtml = `<span class="state-badge state-burned">Burned (Fearless)</span>`;
      }
      if (changedHere) {
        stateHtml += `<span class="this-slot-tag">This slot</span>`;
      }

      const rowClasses = [
        "tell-row",
        "replay-tell-row",
        isStrong ? "row-strong" : "row-lead",
        isOpenActing ? "is-open-acting" : "",
        changedHere ? "is-this-slot" : "",
        curState !== "open" && !changedHere ? "is-unavailable" : "",
      ]
        .filter(Boolean)
        .join(" ");

      return `<tr class="${rowClasses}">
        <td>${signalBadgeHtml(tell.label)}</td>
        <td>
          <div class="champ-cell">
            ${roleBadgeHtml(tell.role)}
            <span class="champ-name">${escapeHtml(tell.champion)}</span>
          </div>
        </td>
        <td class="player-name">${escapeHtml(tell.player_name)}</td>
        <td>${stateHtml}</td>
        <td class="evidence-cell">
          <div class="evidence-words">
            Taken in <strong>${taken}</strong> of <strong>${k}</strong> open games
            <span class="exp-muted">&middot; expected <strong>${formatExpected(expected)}</strong></span>
          </div>
          <div class="rate-bar-wrap" title="Observed ${taken}/${k} vs expected ${formatExpected(expected)}/${k}">
            <div class="rate-bar-obs" style="width: ${obsPct.toFixed(1)}%"></div>
            <div class="rate-bar-exp" style="left: ${expPct.toFixed(1)}%"></div>
          </div>
        </td>
        <td class="num p-val">${formatProb(tell.p_value)}</td>
        <td class="num ${isStrong ? "q-strong" : "q-lead"}">${formatProb(tell.q_value)}</td>
      </tr>`;
    })
    .join("");

  els.replayTellsContainer.innerHTML = `<div class="table-scroll">
    <table class="data-table">
      <thead>
        <tr>
          <th>Tier</th>
          <th>Champion</th>
          <th>Player</th>
          <th>State at slot ${currentSlot ? Number(currentSlot.slot) : 1}</th>
          <th>Observed vs expected (n open games)</th>
          <th class="num">p</th>
          <th class="num">q</th>
        </tr>
      </thead>
      <tbody>${rowsHtml}</tbody>
    </table>
  </div>`;
}

function renderReplaySlotView() {
  if (!state.replayDetail) return;
  const detail = state.replayDetail;
  const g = detail.game || {};
  const sideKey = String(g.side || "").toLowerCase();
  const sideLabel = sideKey === "blue" ? "Blue" : sideKey === "red" ? "Red" : String(g.side || "");
  els.replayGameMeta.textContent =
    `${detail.team_name} (${sideLabel}) vs ${g.opponent_name || g.opponent_id} · Game ${g.game_num} · ${g.played_at || "—"} · Patch ${g.patch || "—"}`;
  els.replayMinNNote.textContent =
    detail.note || "We only show tells with at least 4 qualifying games.";

  renderReplayBurnedBar(detail);
  renderReplayDraftStrip(detail);
  renderReplayTellsPanel(detail);
  showReplayBody();
}

async function loadReplayGame(teamId, gameId, updateHash = true) {
  const tid = String(teamId);
  const gid = String(gameId);
  const reqId = ++state.replayReqSeq;

  state.replayGameId = gid;
  state.replayDetail = null;
  state.replaySlotIdx = 0;
  els.replayNextOpenTellBtn.disabled = true;
  renderReplayGamePicker();
  if (updateHash) updateUrlHash();
  showReplayStatus(`Loading 20-slot draft replay…`);

  try {
    const detail = await fetchJson(
      `/replay/${encodeURIComponent(tid)}/${encodeURIComponent(gid)}`
    );
    if (reqId !== state.replayReqSeq || state.activeTeamId !== tid) return;
    state.replayDetail = detail;
    state.replaySlotIdx = 0;
    renderReplaySlotView();
  } catch (err) {
    if (reqId !== state.replayReqSeq) return;
    if (err.status === 503) {
      render503State(err.detail);
    } else if (err.status === 404) {
      showReplayStatus(
        `<strong>Game not available for Draft Replay (404).</strong> ${escapeHtml(err.detail || err.message)}`
      );
    } else {
      showReplayStatus(
        `<strong>Could not load Draft Replay.</strong> ${escapeHtml(err.detail || err.message)}`
      );
    }
  }
}

async function loadReplayForTeam(teamId, preferredGameId = null, updateHash = true) {
  const tid = String(teamId);
  if (!isTargetTeam(tid)) return;

  // Reuse already-loaded games list for this team if we already have it
  if (state.replayTeamId === tid && state.replayGames.length > 0 && !preferredGameId && state.replayDetail) {
    renderReplayGamePicker();
    renderReplaySlotView();
    if (updateHash) updateUrlHash();
    return;
  }

  const reqId = ++state.replayReqSeq;
  state.replayTeamId = tid;
  state.replayGames = [];
  state.replayGameId = null;
  state.replayDetail = null;
  state.replaySlotIdx = 0;
  els.replayGamePicker.innerHTML = `<div class="empty-table-msg">Loading recent games…</div>`;
  els.replayGamesCount.textContent = "…";
  showReplayStatus("Loading recent complete games…");

  try {
    const data = await fetchJson(`/replay/${encodeURIComponent(tid)}/games`);
    if (reqId !== state.replayReqSeq || state.activeTeamId !== tid) return;

    const games = Array.isArray(data.games) ? data.games : [];
    state.replayGames = games;
    renderReplayGamePicker();

    if (games.length === 0) {
      showReplayStatus("No complete games available for Draft Replay for this team.");
      if (updateHash) updateUrlHash();
      return;
    }

    const chosen =
      preferredGameId && games.some((g) => g.game_id === preferredGameId)
        ? preferredGameId
        : games[0].game_id;
    await loadReplayGame(tid, chosen, updateHash);
  } catch (err) {
    if (reqId !== state.replayReqSeq) return;
    if (err.status === 503) {
      render503State(err.detail);
    } else {
      els.replayGamePicker.innerHTML = "";
      showReplayStatus(
        `<strong>Could not load recent games.</strong> ${escapeHtml(err.detail || err.message)}`
      );
    }
  }
}

function setActiveView(viewName, preferredGameId = null, updateHash = true) {
  const canReplay = isTargetTeam(state.activeTeamId);
  const nextView = viewName === "replay" && canReplay ? "replay" : "silence";
  state.activeView = nextView;
  syncActiveViewTabs();
  if (updateHash) updateUrlHash();
  if (nextView === "replay" && state.activeTeamId) {
    loadReplayForTeam(state.activeTeamId, preferredGameId, updateHash);
  }
}

/* ---------- Full Report Render ---------- */

function renderReport(report, preferredReplayGameId = null) {
  state.report = report;

  const isSelf = state.selfScoutMode;
  document.body.classList.toggle("is-self-scout", isSelf);
  els.selfScoutToggle.setAttribute("aria-checked", isSelf ? "true" : "false");
  els.selfScoutBanner.classList.toggle("hidden", !isSelf);

  els.reportModeEyebrow.textContent = isSelf
    ? "Self-scout · what opponents can read about us"
    : "Opponent silence report";
  els.reportTeamName.textContent = report.team_name || report.team_id;

  const tells = Array.isArray(report.left_open) ? report.left_open : [];
  const bans = Array.isArray(report.early_ban) ? report.early_ban : [];
  const strongCount = tells.filter((t) => t.label === "strong").length;
  const leadCount = tells.length - strongCount;

  const hasStrong = strongCount > 0;
  els.kpiStrongCount.textContent = String(strongCount);
  els.kpiStrongChip.classList.toggle("kpi-strong-zero", !hasStrong);
  els.kpiStrongSub.classList.toggle("hidden", hasStrong);
  els.kpiLeadCount.textContent = String(leadCount);
  els.kpiEarlyBanCount.textContent = String(bans.length);
  if (els.kpiPatternsTestedCount) {
    els.kpiPatternsTestedCount.textContent =
      state.totalTests !== null ? state.totalTests.toLocaleString("en-US") : "—";
  }

  els.reportMinNNote.textContent =
    report.note || "Only tells with at least 4 qualifying games are shown.";

  renderRecommendedActions(report);
  syncTellFiltersForReport(report);
  renderLeftOpenTells(report);
  renderEarlyBanTable(report);
  syncActiveViewTabs();
  renderTeamList();
  showReportContent();

  if (state.activeView === "replay" && isTargetTeam(state.activeTeamId)) {
    loadReplayForTeam(state.activeTeamId, preferredReplayGameId, false);
  }
}

/* ---------- Navigation & Data Loading ---------- */

function resetReportFilters() {
  state.roleFilter = "all";
  state.playerFilter = "all";
  state.tierFilter = "all";
  state.banQuery = "";
  state.replayFilter = "all";
  els.banSearch.value = "";
}

function resetReplayCacheIfTeamChanged(nextTeamId) {
  if (state.replayTeamId !== String(nextTeamId)) {
    state.replayTeamId = null;
    state.replayGames = [];
    state.replayGameId = null;
    state.replayDetail = null;
    state.replaySlotIdx = 0;
  }
}

async function loadSelfScout(updateHash = true, targetView = "silence", replayGameId = null) {
  const reqId = ++state.requestSeq;
  state.selfScoutMode = true;
  state.activeTeamId = SELF_SCOUT_TEAM_ID;
  state.activeView = targetView === "replay" ? "replay" : "silence";
  resetReplayCacheIfTeamChanged(SELF_SCOUT_TEAM_ID);
  document.body.classList.add("is-self-scout");
  els.selfScoutToggle.setAttribute("aria-checked", "true");
  els.selfScoutBanner.classList.remove("hidden");
  renderTeamList();

  if (updateHash) {
    state.replayGameId = replayGameId;
    updateUrlHash();
  }

  resetReportFilters();
  renderLoadingState("Loading Self-Scout: Cloud9 Kia…");

  try {
    const report = await fetchJson("/self-scout");
    if (reqId !== state.requestSeq) return;
    renderReport(report, replayGameId);
  } catch (err) {
    if (reqId !== state.requestSeq) return;
    if (err.status === 503) {
      render503State(err.detail);
    } else if (err.status === 404) {
      render404State(SELF_SCOUT_TEAM_ID, err.detail);
    } else {
      renderGenericError(err.detail || err.message);
    }
  }
}

async function loadTeamReport(teamId, updateHash = true, targetView = "silence", replayGameId = null) {
  const tid = String(teamId);
  const reqId = ++state.requestSeq;
  state.selfScoutMode = false;
  state.activeTeamId = tid;
  state.activeView = targetView === "replay" && isTargetTeam(tid) ? "replay" : "silence";
  resetReplayCacheIfTeamChanged(tid);
  if (tid !== SELF_SCOUT_TEAM_ID) {
    state.lastOpponentTeamId = tid;
  }
  document.body.classList.remove("is-self-scout");
  els.selfScoutToggle.setAttribute("aria-checked", "false");
  els.selfScoutBanner.classList.add("hidden");
  renderTeamList();

  if (updateHash) {
    state.replayGameId = replayGameId;
    updateUrlHash();
  }

  resetReportFilters();
  renderLoadingState(`Loading Silence Report…`);

  try {
    const report = await fetchJson(`/silence/${encodeURIComponent(tid)}`);
    if (reqId !== state.requestSeq) return;
    renderReport(report, replayGameId);
  } catch (err) {
    if (reqId !== state.requestSeq) return;
    if (err.status === 503) {
      render503State(err.detail);
    } else if (err.status === 404) {
      render404State(tid, err.detail);
    } else {
      renderGenericError(err.detail || err.message);
    }
  }
}

function renderTotalTestsCount(totalTests) {
  const num = Number(totalTests);
  if (!Number.isFinite(num) || num < 0) return;
  const formatted = num.toLocaleString("en-US");
  document.querySelectorAll(".total-tests-count").forEach((el) => {
    el.textContent = formatted;
  });
  if (els.kpiPatternsTestedCount) {
    els.kpiPatternsTestedCount.textContent = formatted;
  }
}

function getDefaultOpponentId() {
  // With no explicit hash, pick the first TARGET_TEAMS opponent (excluding Cloud9 Kia)
  // that has at least one strong tell, while keeping the rail order untouched.
  const teamsById = new Map(state.teams.map((t) => [String(t.team_id), t]));
  for (const tid of TARGET_TEAMS_ORDER) {
    if (tid === SELF_SCOUT_TEAM_ID) continue;
    const team = teamsById.get(tid);
    if (team && Number(team.strong_count) > 0) {
      return tid;
    }
  }
  const firstOpponent = state.teams.find((t) => String(t.team_id) !== SELF_SCOUT_TEAM_ID);
  if (firstOpponent) return String(firstOpponent.team_id);
  return state.teams.length > 0 ? String(state.teams[0].team_id) : SELF_SCOUT_TEAM_ID;
}

function routeFromHash() {
  const hash = window.location.hash || "";
  if (hash === "#/self-scout") {
    loadSelfScout(false, "silence");
    return;
  }
  const selfReplayMatch = hash.match(/^#\/self-scout\/replay(?:\/(.+))?$/);
  if (selfReplayMatch) {
    const gid = selfReplayMatch[1] ? decodeURIComponent(selfReplayMatch[1]) : null;
    loadSelfScout(false, "replay", gid);
    return;
  }
  const teamReplayMatch = hash.match(/^#\/team\/([^/]+)\/replay(?:\/(.+))?$/);
  if (teamReplayMatch) {
    const tid = decodeURIComponent(teamReplayMatch[1]);
    const gid = teamReplayMatch[2] ? decodeURIComponent(teamReplayMatch[2]) : null;
    loadTeamReport(tid, false, "replay", gid);
    return;
  }
  const match = hash.match(/^#\/team\/(.+)$/);
  if (match) {
    loadTeamReport(decodeURIComponent(match[1]), false, "silence");
    return;
  }
  if (state.teams.length > 0) {
    loadTeamReport(getDefaultOpponentId(), true, "silence");
  }
}

async function initApp() {
  els.dbStatusPill.textContent = "Connecting…";
  els.dbStatusPill.className = "status-pill";
  renderLoadingState("Loading teams…");

  try {
    const [teams, meta] = await Promise.all([
      fetchJson("/teams"),
      fetchJson("/meta").catch(() => null),
    ]);
    state.teams = Array.isArray(teams) ? teams : [];
    if (meta && Number.isFinite(Number(meta.total_tests))) {
      state.totalTests = Number(meta.total_tests);
      renderTotalTestsCount(state.totalTests);
    }
    els.dbStatusPill.textContent = `${state.teams.length} teams ready`;
    els.dbStatusPill.className = "status-pill is-ok";
    renderTeamList();

    if (state.teams.length === 0) {
      showStatePanel(
        `<div class="state-code">EMPTY DATABASE</div>
         <h2 class="state-title">No teams found in derived/unpicked.duckdb</h2>
         <p class="state-detail">Run the build, baseline, and signals pipeline to populate scouting data.</p>`
      );
      return;
    }

    routeFromHash();
  } catch (err) {
    els.teamList.innerHTML = `<div class="rail-placeholder">Unavailable</div>`;
    if (err.status === 503) {
      render503State(err.detail);
    } else {
      els.dbStatusPill.textContent = "API error";
      els.dbStatusPill.className = "status-pill is-error";
      renderGenericError(err.detail || err.message);
    }
  }
}

/* ---------- Event Listeners ---------- */

els.selfScoutToggle.addEventListener("click", () => {
  if (state.selfScoutMode) {
    const fallback = state.lastOpponentTeamId || getDefaultOpponentId();
    loadTeamReport(fallback, true);
  } else {
    loadSelfScout(true);
  }
});

els.teamSearch.addEventListener("input", (e) => {
  state.teamQuery = e.target.value;
  renderTeamList();
});

els.teamList.addEventListener("click", (e) => {
  const btn = e.target.closest(".team-item");
  if (!btn) return;
  const tid = btn.dataset.teamId;
  if (tid) {
    loadTeamReport(tid, true);
  }
});

els.rolePills.addEventListener("click", (e) => {
  const btn = e.target.closest(".pill[data-role]");
  if (!btn || !state.report) return;
  state.roleFilter = btn.dataset.role;
  syncTellFiltersForReport(state.report);
  renderLeftOpenTells(state.report);
});

els.playerFilter.addEventListener("change", (e) => {
  if (!state.report) return;
  state.playerFilter = e.target.value;
  renderLeftOpenTells(state.report);
});

els.tierPills.addEventListener("click", (e) => {
  const btn = e.target.closest(".pill[data-tier]");
  if (!btn || !state.report) return;
  state.tierFilter = btn.dataset.tier;
  syncTellFiltersForReport(state.report);
  renderLeftOpenTells(state.report);
});

els.groupPills.addEventListener("click", (e) => {
  const btn = e.target.closest(".pill[data-group]");
  if (!btn || !state.report) return;
  state.groupBy = btn.dataset.group;
  syncTellFiltersForReport(state.report);
  renderLeftOpenTells(state.report);
});

els.earlyOnlyToggle.addEventListener("change", (e) => {
  state.earlyOnly = Boolean(e.target.checked);
  if (state.report) renderEarlyBanTable(state.report);
});

els.banSearch.addEventListener("input", (e) => {
  state.banQuery = e.target.value;
  if (state.report) renderEarlyBanTable(state.report);
});

els.viewTabs.addEventListener("click", (e) => {
  const btn = e.target.closest(".view-tab[data-view]");
  if (!btn || btn.classList.contains("hidden")) return;
  setActiveView(btn.dataset.view, null, true);
});

els.replayGamePicker.addEventListener("click", (e) => {
  const card = e.target.closest(".game-card[data-game-id]");
  if (!card || !state.activeTeamId) return;
  const gid = card.dataset.gameId;
  if (gid && gid !== state.replayGameId) {
    loadReplayGame(state.activeTeamId, gid, true);
  }
});

els.replayPrevBtn.addEventListener("click", () => {
  if (!state.replayDetail || state.replaySlotIdx <= 0) return;
  state.replaySlotIdx -= 1;
  renderReplaySlotView();
});

els.replayNextBtn.addEventListener("click", () => {
  if (!state.replayDetail) return;
  const maxIdx = Math.max(0, (state.replayDetail.slots || []).length - 1);
  if (state.replaySlotIdx >= maxIdx) return;
  state.replaySlotIdx += 1;
  renderReplaySlotView();
});

els.replayNextOpenTellBtn.addEventListener("click", () => {
  advanceToNextOpenTellSlot();
});

els.replayDraftStrip.addEventListener("click", (e) => {
  const cell = e.target.closest(".draft-cell[data-slot-idx]");
  if (!cell || !state.replayDetail) return;
  const idx = Number(cell.dataset.slotIdx);
  if (Number.isInteger(idx) && idx >= 0 && idx < (state.replayDetail.slots || []).length) {
    state.replaySlotIdx = idx;
    renderReplaySlotView();
  }
});

els.replayStatePills.addEventListener("click", (e) => {
  const btn = e.target.closest(".pill[data-replay-filter]");
  if (!btn || !state.replayDetail) return;
  state.replayFilter = btn.dataset.replayFilter;
  renderReplayTellsPanel(state.replayDetail);
});

els.statePanel.addEventListener("click", (e) => {
  const btn = e.target.closest("[data-action]");
  if (!btn) return;
  const action = btn.dataset.action;
  if (action === "retry-init") {
    initApp();
  } else if (action === "select-first-team") {
    if (state.teams.length > 0) {
      loadTeamReport(getDefaultOpponentId(), true);
    } else {
      initApp();
    }
  } else if (action === "open-self-scout") {
    loadSelfScout(true);
  }
});

window.addEventListener("hashchange", () => {
  if (state.teams.length > 0) {
    routeFromHash();
  }
});

document.addEventListener("keydown", (e) => {
  const activeTag = document.activeElement ? document.activeElement.tagName : "";
  const inInput = ["INPUT", "TEXTAREA", "SELECT"].includes(activeTag);
  if (inInput) return;

  if (e.key === "/") {
    e.preventDefault();
    els.teamSearch.focus();
    els.teamSearch.select();
    return;
  }

  if (state.activeView === "replay" && state.replayDetail) {
    const slots = state.replayDetail.slots || [];
    if (e.key === "ArrowLeft" && state.replaySlotIdx > 0) {
      e.preventDefault();
      state.replaySlotIdx -= 1;
      renderReplaySlotView();
    } else if (e.key === "ArrowRight" && state.replaySlotIdx < slots.length - 1) {
      e.preventDefault();
      state.replaySlotIdx += 1;
      renderReplaySlotView();
    } else if (
      (e.key === "n" || e.key === "N") &&
      !e.ctrlKey &&
      !e.metaKey &&
      !e.altKey
    ) {
      if (advanceToNextOpenTellSlot()) {
        e.preventDefault();
      }
    }
  }
});

initApp();

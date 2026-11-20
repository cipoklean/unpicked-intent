"use strict";

/* Unpicked Intent — Silence Report Frontend
 * Plain offline JS talking strictly to FastAPI endpoints:
 *   GET /teams
 *   GET /silence/{team_id}
 *   GET /self-scout
 */

const TARGET_TEAM_IDS = new Set([
  "340",
  "47363",
  "54198",
  "53073",
  "52416",
  "47499",
  "56389",
  "47351",
]);
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
  top: "TOP",
  jungle: "JGL",
  mid: "MID",
  bot: "BOT",
  support: "SUP",
};

const state = {
  teams: [],
  teamQuery: "",
  activeTeamId: null,
  lastOpponentTeamId: null,
  selfScoutMode: false,
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
  kpiStrongCount: document.getElementById("kpi-strong-count"),
  kpiLeadCount: document.getElementById("kpi-lead-count"),
  kpiEarlyBanCount: document.getElementById("kpi-early-ban-count"),
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
  if (endpoint === "/teams") return "data/teams.json";
  if (endpoint === "/self-scout") return "data/self-scout.json";
  if (endpoint.startsWith("/silence/")) {
    const teamId = endpoint.slice("/silence/".length);
    return `data/silence/${teamId}.json`;
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
    err.status = IS_STATIC && endpoint.startsWith("/silence/") ? 404 : 500;
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
     <pre class="code-block">python -m unpicked_intent.pipeline.build --grid &lt;path-to-grid-repo&gt;
python -m unpicked_intent.roles.infer_players --grid &lt;path-to-grid-repo&gt;
python -m unpicked_intent.scoring.baseline
python -m unpicked_intent.scoring.signals --grid &lt;path-to-grid-repo&gt;</pre>
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
    parts.push(`<div class="rail-section-label">LCS Targets (${targetTeams.length})</div>`);
    parts.push(...targetTeams.map(renderTeamButtonHtml));
  }
  if (otherTeams.length > 0) {
    parts.push(`<div class="rail-section-label">All Other Teams (${otherTeams.length})</div>`);
    parts.push(...otherTeams.map(renderTeamButtonHtml));
  }
  els.teamList.innerHTML = parts.join("");
}

function renderTeamButtonHtml(team) {
  const tid = String(team.team_id);
  const isActive =
    (state.selfScoutMode && tid === SELF_SCOUT_TEAM_ID) ||
    (!state.selfScoutMode && state.activeTeamId === tid);
  const c9Tag = tid === SELF_SCOUT_TEAM_ID ? `<span class="c9-tag">SELF</span>` : "";

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
    <td class="num">${k}</td>
    <td class="num">${taken}</td>
    <td class="num">${formatExpected(expected)}</td>
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
          <th>Observed vs. Expected (n open games)</th>
          <th class="num">n</th>
          <th class="num">Obs</th>
          <th class="num">Exp</th>
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
        <th class="num">Team Avg Slot</th>
        <th class="num">Meta Avg Slot</th>
        <th>Slot Shift (1 &rarr; 20)</th>
        <th class="num">Delta</th>
      </tr>
    </thead>
    <tbody>${rowsHtml}</tbody>
  </table>`;
}

/* ---------- Full Report Render ---------- */

function renderReport(report) {
  state.report = report;

  const isSelf = state.selfScoutMode;
  document.body.classList.toggle("is-self-scout", isSelf);
  els.selfScoutToggle.setAttribute("aria-checked", isSelf ? "true" : "false");
  els.selfScoutBanner.classList.toggle("hidden", !isSelf);

  els.reportModeEyebrow.textContent = isSelf
    ? "SELF-SCOUT · WHAT OPPONENTS CAN READ ABOUT US"
    : "OPPONENT SILENCE REPORT";
  els.reportTeamName.textContent = report.team_name || report.team_id;

  const tells = Array.isArray(report.left_open) ? report.left_open : [];
  const bans = Array.isArray(report.early_ban) ? report.early_ban : [];
  const strongCount = tells.filter((t) => t.label === "strong").length;
  const leadCount = tells.length - strongCount;

  els.kpiStrongCount.textContent = String(strongCount);
  els.kpiLeadCount.textContent = String(leadCount);
  els.kpiEarlyBanCount.textContent = String(bans.length);

  els.reportMinNNote.textContent =
    report.note || "Only tells with at least 4 qualifying games are shown.";

  renderRecommendedActions(report);
  syncTellFiltersForReport(report);
  renderLeftOpenTells(report);
  renderEarlyBanTable(report);
  renderTeamList();
  showReportContent();
}

/* ---------- Navigation & Data Loading ---------- */

function resetReportFilters() {
  state.roleFilter = "all";
  state.playerFilter = "all";
  state.tierFilter = "all";
  state.banQuery = "";
  els.banSearch.value = "";
}

async function loadSelfScout(updateHash = true) {
  const reqId = ++state.requestSeq;
  state.selfScoutMode = true;
  state.activeTeamId = SELF_SCOUT_TEAM_ID;
  document.body.classList.add("is-self-scout");
  els.selfScoutToggle.setAttribute("aria-checked", "true");
  els.selfScoutBanner.classList.remove("hidden");
  renderTeamList();

  if (updateHash && window.location.hash !== "#/self-scout") {
    window.history.replaceState(null, "", "#/self-scout");
  }

  resetReportFilters();
  renderLoadingState("Loading Self-Scout: Cloud9 Kia…");

  try {
    const report = await fetchJson("/self-scout");
    if (reqId !== state.requestSeq) return;
    renderReport(report);
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

async function loadTeamReport(teamId, updateHash = true) {
  const tid = String(teamId);
  const reqId = ++state.requestSeq;
  state.selfScoutMode = false;
  state.activeTeamId = tid;
  if (tid !== SELF_SCOUT_TEAM_ID) {
    state.lastOpponentTeamId = tid;
  }
  document.body.classList.remove("is-self-scout");
  els.selfScoutToggle.setAttribute("aria-checked", "false");
  els.selfScoutBanner.classList.add("hidden");
  renderTeamList();

  const targetHash = `#/team/${encodeURIComponent(tid)}`;
  if (updateHash && window.location.hash !== targetHash) {
    window.history.replaceState(null, "", targetHash);
  }

  resetReportFilters();
  renderLoadingState(`Loading Silence Report…`);

  try {
    const report = await fetchJson(`/silence/${encodeURIComponent(tid)}`);
    if (reqId !== state.requestSeq) return;
    renderReport(report);
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

function getDefaultOpponentId() {
  const firstOpponent = state.teams.find((t) => String(t.team_id) !== SELF_SCOUT_TEAM_ID);
  if (firstOpponent) return String(firstOpponent.team_id);
  return state.teams.length > 0 ? String(state.teams[0].team_id) : SELF_SCOUT_TEAM_ID;
}

function routeFromHash() {
  const hash = window.location.hash || "";
  if (hash === "#/self-scout") {
    loadSelfScout(false);
    return;
  }
  const match = hash.match(/^#\/team\/(.+)$/);
  if (match) {
    loadTeamReport(decodeURIComponent(match[1]), false);
    return;
  }
  if (state.teams.length > 0) {
    loadTeamReport(getDefaultOpponentId(), true);
  }
}

async function initApp() {
  els.dbStatusPill.textContent = "Connecting…";
  els.dbStatusPill.className = "status-pill";
  renderLoadingState("Loading teams…");

  try {
    const teams = await fetchJson("/teams");
    state.teams = Array.isArray(teams) ? teams : [];
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
  if (
    e.key === "/" &&
    document.activeElement &&
    !["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement.tagName)
  ) {
    e.preventDefault();
    els.teamSearch.focus();
    els.teamSearch.select();
  }
});

initApp();

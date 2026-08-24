// ==================================================================
// Job Scheduler Dashboard — vanilla JS, no build step.
// Talks to the Go API over fetch(). State lives in memory (no
// localStorage per artifact rules... but this ISN'T an artifact,
// it's a real file you'll run from your own server, so a plain JS
// var is fine here — kept simple on purpose).
// ==================================================================

const API_BASE = ""; // same-origin: API server also serves this file from ./web

const state = {
  token: null,
  projects: [],
  currentProjectId: null,
  queues: [],
  jobsPage: 1,
  pollHandle: null,
};

// ---------------- API helper ----------------

async function api(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (state.token) headers["Authorization"] = "Bearer " + state.token;

  const res = await fetch(API_BASE + path, { ...options, headers });
  if (res.status === 401) {
    logout();
    throw new Error("Session expired, please log in again");
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `Request failed (${res.status})`);
  return data;
}

// ---------------- Auth ----------------

const authForm = document.getElementById("auth-form");
const tabLogin = document.getElementById("tab-login");
const tabRegister = document.getElementById("tab-register");
let authMode = "login";

tabLogin.onclick = () => setAuthMode("login");
tabRegister.onclick = () => setAuthMode("register");

function setAuthMode(mode) {
  authMode = mode;
  tabLogin.classList.toggle("active", mode === "login");
  tabRegister.classList.toggle("active", mode === "register");
  document.getElementById("auth-submit").textContent = mode === "login" ? "Login" : "Create account";
}

authForm.onsubmit = async (e) => {
  e.preventDefault();
  const email = document.getElementById("auth-email").value;
  const password = document.getElementById("auth-password").value;
  const errEl = document.getElementById("auth-error");
  errEl.textContent = "";
  try {
    const path = authMode === "login" ? "/auth/login" : "/auth/register";
    const data = await api(path, { method: "POST", body: JSON.stringify({ email, password }) });
    state.token = data.token;
    await enterApp();
  } catch (err) {
    errEl.textContent = err.message;
  }
};

function logout() {
  state.token = null;
  stopPolling();
  document.getElementById("app").classList.add("hidden");
  document.getElementById("auth-screen").classList.remove("hidden");
}
document.getElementById("logout-btn").onclick = logout;

// ---------------- App entry ----------------

async function enterApp() {
  document.getElementById("auth-screen").classList.add("hidden");
  document.getElementById("app").classList.remove("hidden");
  await loadProjects();
}

async function loadProjects() {
  state.projects = await api("/projects");
  const select = document.getElementById("project-select");
  select.innerHTML = "";
  if (state.projects.length === 0) {
    select.innerHTML = `<option value="">No projects yet — create one</option>`;
    return;
  }
  for (const p of state.projects) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = p.name;
    select.appendChild(opt);
  }
  state.currentProjectId = state.projects[0].id;
  select.value = state.currentProjectId;
  await refreshCurrentView();
  startPolling();
}

document.getElementById("project-select").onchange = async (e) => {
  state.currentProjectId = e.target.value;
  await refreshCurrentView();
};

// ---------------- Nav ----------------

document.querySelectorAll(".nav-link").forEach((btn) => {
  btn.onclick = () => switchView(btn.dataset.view);
});

function switchView(view) {
  document.querySelectorAll(".nav-link").forEach((b) => b.classList.toggle("active", b.dataset.view === view));
  document.querySelectorAll(".view").forEach((v) => v.classList.add("hidden"));
  document.getElementById("view-" + view).classList.remove("hidden");
  refreshCurrentView();
}

function currentView() {
  return document.querySelector(".nav-link.active").dataset.view;
}

async function refreshCurrentView() {
  if (!state.currentProjectId) return;
  const view = currentView();
  try {
    if (view === "overview") await renderOverview();
    else if (view === "queues") await renderQueues();
    else if (view === "jobs") await renderJobs();
    else if (view === "workers") await renderWorkers();
  } catch (err) {
    console.error(err);
  }
}

// Light polling so the dashboard feels "live" without WebSockets.
function startPolling() {
  stopPolling();
  state.pollHandle = setInterval(refreshCurrentView, 4000);
}
function stopPolling() {
  if (state.pollHandle) clearInterval(state.pollHandle);
}

// ---------------- Overview ----------------

async function renderOverview() {
  const data = await api(`/projects/${state.currentProjectId}/dashboard`);
  const grid = document.getElementById("stat-grid");
  grid.innerHTML = `
    <div class="stat-card"><div class="value">${data.queue_count}</div><div class="label">Queues</div></div>
    <div class="stat-card"><div class="value">${data.online_worker_count} / ${data.worker_count}</div><div class="label">Workers online</div></div>
    <div class="stat-card"><div class="value">${data.completed_last_hour}</div><div class="label">Completed (last hour)</div></div>
    <div class="stat-card"><div class="value">${data.dlq_pending_count}</div><div class="label">In Dead Letter Queue</div></div>
  `;

  const counts = data.job_counts_by_status || {};
  const total = Object.values(counts).reduce((a, b) => a + b, 0) || 1;
  const colors = {
    queued: "var(--accent)", scheduled: "var(--accent)", claimed: "var(--yellow)",
    running: "var(--yellow)", completed: "var(--green)", failed: "var(--red)",
    dead_letter: "var(--red)", cancelled: "var(--purple)",
  };
  const bars = document.getElementById("status-bars");
  bars.innerHTML = Object.entries(counts).map(([status, count]) => `
    <div class="status-bar-row">
      <div class="name">${status.replace("_", " ")}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${(count/total*100).toFixed(1)}%; background:${colors[status] || "var(--accent)"}"></div></div>
      <div class="count">${count}</div>
    </div>
  `).join("") || `<p style="color:var(--text-dim)">No jobs yet.</p>`;
}

// ---------------- Queues ----------------

async function renderQueues() {
  state.queues = await api(`/projects/${state.currentProjectId}/queues`);
  const tbody = document.getElementById("queues-table-body");
  tbody.innerHTML = state.queues.map((q) => `
    <tr>
      <td>${escapeHtml(q.name)}</td>
      <td>${q.priority}</td>
      <td>${q.concurrency_limit}</td>
      <td>${q.retry_strategy}</td>
      <td><span class="badge ${q.is_paused ? "badge-failed" : "badge-completed"}">${q.is_paused ? "Paused" : "Active"}</span></td>
      <td class="actions-cell">
        <button class="btn-secondary" onclick="toggleQueuePause('${q.id}', ${!q.is_paused})">${q.is_paused ? "Resume" : "Pause"}</button>
      </td>
    </tr>
  `).join("") || `<tr><td colspan="6" style="color:var(--text-dim)">No queues yet.</td></tr>`;

  // Keep job filter + new-job modal queue dropdowns in sync
  const options = state.queues.map((q) => `<option value="${q.id}">${escapeHtml(q.name)}</option>`).join("");
  document.getElementById("job-queue-filter").innerHTML = `<option value="">All queues</option>` + options;
  document.getElementById("nj-queue").innerHTML = options;
}

async function toggleQueuePause(queueId, pause) {
  await api(`/queues/${queueId}/${pause ? "pause" : "resume"}`, { method: "PATCH" });
  renderQueues();
}

// ---------------- Jobs ----------------

async function renderJobs() {
  if (state.queues.length === 0) state.queues = await api(`/projects/${state.currentProjectId}/queues`);
  const queueId = document.getElementById("job-queue-filter").value;
  const status = document.getElementById("job-status-filter").value;
  const tbody = document.getElementById("jobs-table-body");

  if (!queueId && state.queues.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" style="color:var(--text-dim)">Create a queue first.</td></tr>`;
    return;
  }

  // job endpoints are per-queue; if "All queues" selected, fetch each and merge (fine at demo scale)
  const targetQueues = queueId ? [queueId] : state.queues.map((q) => q.id);
  let allJobs = [];
  for (const qid of targetQueues) {
    const qs = new URLSearchParams({ page: state.jobsPage, page_size: 25 });
    if (status) qs.set("status", status);
    const data = await api(`/queues/${qid}/jobs?${qs.toString()}`);
    allJobs = allJobs.concat((data.jobs || []).map((j) => ({ ...j, _queueId: qid })));
  }
  allJobs.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

  tbody.innerHTML = allJobs.map((j) => `
    <tr>
      <td>${escapeHtml(j.job_type)}</td>
      <td><span class="badge badge-${j.status}">${j.status.replace("_"," ")}</span></td>
      <td>${j.priority}</td>
      <td>${j.attempt_count}</td>
      <td>${fmtTime(j.run_at)}</td>
      <td>${fmtTime(j.created_at)}</td>
      <td class="actions-cell">
        <button class="btn-secondary" onclick="openJobDetail('${j.id}')">View</button>
        ${["failed","dead_letter","cancelled"].includes(j.status) ? `<button class="btn-secondary" onclick="retryJob('${j.id}')">Retry</button>` : ""}
      </td>
    </tr>
  `).join("") || `<tr><td colspan="7" style="color:var(--text-dim)">No jobs match this filter.</td></tr>`;

  document.getElementById("jobs-page-label").textContent = `Page ${state.jobsPage}`;
}

document.getElementById("job-queue-filter").onchange = () => { state.jobsPage = 1; renderJobs(); };
document.getElementById("job-status-filter").onchange = () => { state.jobsPage = 1; renderJobs(); };
document.getElementById("jobs-prev-page").onclick = () => { if (state.jobsPage > 1) { state.jobsPage--; renderJobs(); } };
document.getElementById("jobs-next-page").onclick = () => { state.jobsPage++; renderJobs(); };

async function retryJob(jobId) {
  await api(`/jobs/${jobId}/retry`, { method: "POST" });
  renderJobs();
}

async function openJobDetail(jobId) {
  const data = await api(`/jobs/${jobId}`);
  const j = data.job;
  const execs = data.executions || [];
  document.getElementById("job-detail-body").innerHTML = `
    <div class="detail-row"><span class="k">ID</span><span>${j.id}</span></div>
    <div class="detail-row"><span class="k">Type</span><span>${escapeHtml(j.job_type)}</span></div>
    <div class="detail-row"><span class="k">Status</span><span class="badge badge-${j.status}">${j.status}</span></div>
    <div class="detail-row"><span class="k">Attempts</span><span>${j.attempt_count}</span></div>
    <div class="detail-row"><span class="k">Payload</span><span>${escapeHtml(JSON.stringify(JSON.parse(j.payload || "{}")))}</span></div>
    <h4>Execution history</h4>
    ${execs.map((e) => `
      <div class="exec-item">
        <div><strong>Attempt ${e.attempt_number}</strong> — <span class="badge badge-${e.status}">${e.status}</span></div>
        <div>Started: ${fmtTime(e.started_at)} ${e.duration_ms != null ? `— ${e.duration_ms}ms` : ""}</div>
        ${e.error_message ? `<div style="color:var(--red)">Error: ${escapeHtml(e.error_message)}</div>` : ""}
      </div>
    `).join("") || `<p style="color:var(--text-dim)">No execution attempts yet.</p>`}
  `;
  openModal("modal-job-detail");
}

// ---------------- Workers ----------------

async function renderWorkers() {
  const workers = await api(`/projects/${state.currentProjectId}/workers`);
  const tbody = document.getElementById("workers-table-body");
  tbody.innerHTML = workers.map((w) => `
    <tr>
      <td>${escapeHtml(w.hostname)}</td>
      <td><span class="badge badge-${w.is_stale ? "stale" : w.status}">${w.is_stale ? "stale" : w.status}</span></td>
      <td>${w.last_heartbeat_at ? fmtTime(w.last_heartbeat_at) : "—"}</td>
      <td>${fmtTime(w.started_at)}</td>
    </tr>
  `).join("") || `<tr><td colspan="4" style="color:var(--text-dim)">No workers have registered yet.</td></tr>`;
}

// ---------------- Modals ----------------

function openModal(id) {
  document.getElementById("modal-backdrop").classList.remove("hidden");
  document.querySelectorAll(".modal").forEach((m) => m.classList.add("hidden"));
  document.getElementById(id).classList.remove("hidden");
}
function closeModal() {
  document.getElementById("modal-backdrop").classList.add("hidden");
}
document.querySelectorAll("[data-close-modal]").forEach((btn) => (btn.onclick = closeModal));
document.getElementById("modal-backdrop").onclick = (e) => {
  if (e.target.id === "modal-backdrop") closeModal();
};

document.getElementById("new-project-btn").onclick = () => openModal("modal-new-project");
document.getElementById("np-submit").onclick = async () => {
  const name = document.getElementById("np-name").value.trim();
  if (!name) return;
  await api("/projects", { method: "POST", body: JSON.stringify({ name }) });
  closeModal();
  await loadProjects();
};

document.getElementById("new-queue-btn").onclick = () => openModal("modal-new-queue");
document.getElementById("nq-submit").onclick = async () => {
  const body = {
    name: document.getElementById("nq-name").value.trim(),
    priority: Number(document.getElementById("nq-priority").value),
    concurrency_limit: Number(document.getElementById("nq-concurrency").value),
    retry_strategy: document.getElementById("nq-retry-strategy").value,
    max_retries: Number(document.getElementById("nq-max-retries").value),
  };
  if (!body.name) return;
  await api(`/projects/${state.currentProjectId}/queues`, { method: "POST", body: JSON.stringify(body) });
  closeModal();
  renderQueues();
};

document.getElementById("new-job-btn").onclick = async () => {
  if (state.queues.length === 0) state.queues = await api(`/projects/${state.currentProjectId}/queues`);
  document.getElementById("nj-queue").innerHTML = state.queues.map((q) => `<option value="${q.id}">${escapeHtml(q.name)}</option>`).join("");
  openModal("modal-new-job");
};
document.getElementById("nj-submit").onclick = async () => {
  const queueId = document.getElementById("nj-queue").value;
  const jobType = document.getElementById("nj-type").value.trim();
  const payloadRaw = document.getElementById("nj-payload").value.trim() || "{}";
  const priority = Number(document.getElementById("nj-priority").value);
  const runAtRaw = document.getElementById("nj-runat").value;
  const cronRaw = document.getElementById("nj-cron").value.trim();

  if (!queueId || !jobType) return;
  let payload;
  try { payload = JSON.parse(payloadRaw); } catch { alert("Payload must be valid JSON"); return; }

  const body = { job_type: jobType, payload, priority };
  if (runAtRaw) body.run_at = new Date(runAtRaw).toISOString();
  if (cronRaw) body.cron_expr = cronRaw;

  await api(`/queues/${queueId}/jobs`, { method: "POST", body: JSON.stringify(body) });
  closeModal();
  switchView("jobs");
};

// ---------------- Utils ----------------

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function escapeHtml(str) {
  if (str == null) return "";
  return String(str).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

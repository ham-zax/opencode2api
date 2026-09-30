/* ============================================================
   OpenCode2API · app.js (Local API Client)
   ============================================================ */
const $ = id => document.getElementById(id);
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
function toast(msg, type) {
  const el = $('toast');
  if (!el) return;
  el.innerHTML = `<span class="material-symbols-outlined text-[18px] ${type === 'ok' ? 'text-success' : 'text-error'}">${type === 'ok' ? 'check_circle' : 'error'}</span> ${esc(msg)}`;
  el.className = 'fixed bottom-7 left-1/2 -translate-x-1/2 z-[1000] flex items-center gap-2 px-6 py-3 rounded-xl shadow-2xl bg-[#111827] text-white text-sm font-medium';
  clearTimeout(el._t);
  el._t = setTimeout(() => {
    el.className = 'fixed bottom-7 left-1/2 -translate-x-1/2 z-[1000] flex items-center gap-2 px-6 py-3 rounded-xl shadow-2xl bg-[#111827] text-white text-sm font-medium opacity-0 pointer-events-none transition-all duration-300';
  }, 3000);
}

// /api/keys is admin-gated server-side. Hold the admin key in sessionStorage
// and retry once through a prompt when the server answers 401, so the key
// management UI keeps working without baking a credential into the bundle.
const ADMIN_KEY_STORE = 'opencode2api.adminKey';
function getAdminKey() { try { return sessionStorage.getItem(ADMIN_KEY_STORE) || ''; } catch { return ''; } }
function setAdminKey(v) { try { v ? sessionStorage.setItem(ADMIN_KEY_STORE, v) : sessionStorage.removeItem(ADMIN_KEY_STORE); } catch {} }
async function api(u, method, body) {
  const o = { method: method || 'GET', headers: { 'Content-Type': 'application/json' } };
  if (body) o.body = JSON.stringify(body);
  // Only the key-management routes need the admin credential. Sending it to
  // every dashboard call would expose it to every handler and access log for
  // no benefit — the other routes ignore Authorization entirely.
  const needsAdmin = u.startsWith('/api/keys');
  if (needsAdmin) {
    const key = getAdminKey();
    if (key) o.headers['Authorization'] = 'Bearer ' + key;
  }
  let r = await fetch(u, o);
  if (r.status === 401 && needsAdmin) {
    const entered = window.prompt('Admin key required (API_KEY):');
    if (entered) {
      setAdminKey(entered.trim());
      o.headers['Authorization'] = 'Bearer ' + entered.trim();
      r = await fetch(u, o);
    }
  }
  const d = await r.json();
  if (!r.ok) throw new Error(d.error || d.message || 'HTTP ' + r.status);
  return d;
}
function toggleSidebar() {
  const s = $('sidebar'), o = $('sidebarOverlay');
  if (!s) return;
  if (s.classList.contains('open')) { s.classList.remove('open'); if(o)o.style.display='none'; }
  else { s.classList.add('open'); if(o)o.style.display='block'; }
}
function closeSidebar() {
  const s = $('sidebar'), o = $('sidebarOverlay');
  if(s)s.classList.remove('open'); if(o)o.style.display='none';
}
function closeModal() {
  const m = $('modal'), o = $('sidebarOverlay'), s = $('sidebar');
  if(m)m.innerHTML=''; if(m)m.classList.add('hidden');
  if(o)o.style.display='none'; if(s)s.classList.remove('open');
}
function showModal(html) {
  const m = $('modal');
  if (!m) return;
  m.innerHTML = `<div class="absolute inset-0 bg-black/30 backdrop-blur-sm" onclick="closeModal()"></div><div class="relative bg-surface rounded-2xl shadow-2xl w-[480px] max-w-[92vw] max-h-[85vh] overflow-y-auto p-6 animate-modal-in">${html}</div>`;
  const o = $('sidebarOverlay'); if(o)o.style.display='none';
  m.classList.remove('hidden');
}
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });
function toggleTheme() {
  const h = document.documentElement, btn = $('themeBtn');
  if (h.classList.contains('dark')) {
    h.classList.remove('dark'); h.classList.add('light');
    if(btn)btn.innerHTML='<span class="material-symbols-outlined text-xl">dark_mode</span>';
    localStorage.setItem('theme','light');
  } else {
    h.classList.remove('light'); h.classList.add('dark');
    if(btn)btn.innerHTML='<span class="material-symbols-outlined text-xl">light_mode</span>';
    localStorage.setItem('theme','dark');
  }
}
(function(){
  const s = localStorage.getItem('theme');
  if (s === 'dark' || (!s && matchMedia('(prefers-color-scheme:dark)').matches)) {
    document.documentElement.classList.remove('light');
    document.documentElement.classList.add('dark');
    const btn = $('themeBtn'); if(btn)btn.innerHTML='<span class="material-symbols-outlined text-xl">light_mode</span>';
  }
})();

let currentTab = 'dashboard';
const titles = { dashboard:'Dashboard', audit:'Usage Audit', keys:'Key Management', proxies:'Proxy Pool', sources:'Proxy Sources', config:'System Config', logs:'System Logs' };
const loaders = { dashboard: fetchDashboard, audit: fetchAudit, keys: fetchKeys, proxies: fetchProxies, sources: fetchSources, config: fetchConfig, logs: fetchLogs };

document.addEventListener('DOMContentLoaded', function() {
  const nav = $('nav');
  if(nav) nav.addEventListener('click', function(e) {
    const a = e.target.closest('[data-tab]');
    if (!a) return;
    currentTab = a.dataset.tab;
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.add('hidden'));
    const panel = $('tab-' + currentTab); if(panel) panel.classList.remove('hidden');
    document.querySelectorAll('#nav [data-tab]').forEach(x => {
      x.classList.remove('bg-primary-container','text-on-primary-container','font-medium');
      x.classList.add('text-secondary');
    });
    a.classList.remove('text-secondary');
    a.classList.add('bg-primary-container','text-on-primary-container','font-medium');
    const pt = $('pageTitle'); if(pt) pt.textContent = titles[currentTab];
    if (loaders[currentTab]) loaders[currentTab]();
  });
  const first = document.querySelector('#nav [data-tab="dashboard"]');
  if(first) { first.classList.remove('text-secondary'); first.classList.add('bg-primary-container','text-on-primary-container','font-medium'); }
  setTimeout(() => { if(loaders.dashboard) loaders.dashboard(); }, 100);
});

const timers = {};
function startRefresh(tab, fn, interval) {
  if (timers[tab]) clearInterval(timers[tab]);
  timers[tab] = setInterval(() => { if (currentTab === tab) fn(); }, interval);
}

async function doAction(path, btn) {
  const orig = btn.innerHTML;
  btn.innerHTML = '<span class="material-symbols-outlined text-lg animate-spin">sync</span>';
  btn.disabled = true;
  try { await api('/api/' + path, 'POST', {}); toast('Operation successful: '+path, 'ok'); }
  catch (e) { toast('Operation failed: '+e.message, 'err'); }
  btn.innerHTML = orig; btn.disabled = false;
  if (currentTab === 'dashboard') fetchDashboard();
}

// Standby = WARP + the custom fallback proxies. Report which of them answered
// so a press tells you something, rather than the bare "successful" toast the
// generic action handler gives.
async function probeStandby(btn) {
  const orig = btn.innerHTML;
  btn.innerHTML = '<span class="material-symbols-outlined text-lg animate-spin">sensors</span>';
  btn.disabled = true;
  try {
    const r = await api('/api/standby/probe', 'POST', {});
    const parts = [];
    if (r.warp && r.warp.enabled) parts.push('WARP ' + (r.warp.healthy ? 'healthy' : 'unreachable'));
    if (r.custom && r.custom.total) parts.push(r.custom.healthy + '/' + r.custom.total + ' fallback proxies up');
    parts.push(r.readySlots + ' standby slot' + (r.readySlots === 1 ? '' : 's') + ' ready');
    toast(parts.join(' · '), r.readySlots > 0 ? 'ok' : 'err');
  } catch (e) { toast('Probe failed: ' + e.message, 'err'); }
  btn.innerHTML = orig; btn.disabled = false;
  if (currentTab === 'dashboard') fetchDashboard();
}

function statCard(icon, label, value, sub, color) {
  const colors = { primary:'bg-primary/10 text-primary', success:'bg-success/10 text-success', warning:'bg-warning/10 text-warning', info:'bg-info/10 text-info', red:'bg-error/10 text-error' };
  const c = colors[color] || colors.primary;
  return `<div class="bg-surface rounded-xl border border-border p-5 ambient-shadow hover:shadow-md hover:border-primary/30 transition-all">
      <div class="flex items-center gap-3 mb-2">
        <div class="w-9 h-9 rounded-full ${c} flex items-center justify-center"><span class="material-symbols-outlined text-lg">${icon}</span></div>
        <span class="text-label-md text-secondary">${esc(label)}</span>
      </div>
      <div class="flex items-end gap-2 mt-2">
        <span class="text-display-lg text-on-surface font-bold tracking-tight">${esc(value == null ? '0' : String(value))}</span>
        ${sub != null ? `<span class="text-body-md text-secondary mb-1">/ ${esc(String(sub))}</span>` : ''}
      </div>
    </div>`;
}
function badge(text, type) {
  const t = { green:'bg-success/10 text-success', amber:'bg-warning/10 text-warning', red:'bg-error/10 text-error', blue:'bg-info/10 text-info', gray:'bg-surface-container-high text-secondary', active:'bg-success/10 text-success', pending:'bg-warning/10 text-warning', disabled:'bg-surface-container-high text-secondary' };
  return `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-label-md font-medium ${t[type] || t.gray}">${esc(text)}</span>`;
}
function renderTable(headers, rows, emptyMsg) {
  if (!rows || !rows.length) return `<div class="text-center py-12"><span class="material-symbols-outlined text-5xl text-secondary/30 block mb-3">database</span><p class="text-body-md text-secondary">${esc(emptyMsg || 'No data available')}</p></div>`;
  return `<table class="w-full text-body-md"><thead><tr class="border-b border-outline-variant bg-surface-container-low">${headers.map(h => `<th class="text-left px-4 py-3 text-label-md text-secondary font-semibold">${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows}</tbody></table>`;
}
function statRow(cells) {
  return `<tr class="border-b border-outline-variant hover:bg-surface-container-low transition-colors">${cells.map(c => `<td class="px-4 py-3">${c}</td>`).join('')}</tr>`;
}
function skeletonCards(count) {
  let h = '';
  for(let i=0;i<count;i++) h += `<div class="bg-surface rounded-xl border border-border p-5 ambient-shadow animate-pulse"><div class="h-3 w-20 bg-surface-container-high rounded mb-3"></div><div class="h-7 w-14 bg-surface-container-high rounded"></div></div>`;
  return h;
}

/* ===== DASHBOARD ===== */
async function fetchDashboard() {
  startRefresh('dashboard', fetchDashboard, 10000);
  try {
    const s = await api('/api/status');
    const st = s.stats || {};
    const sc = $('statCards');
    if(sc) sc.innerHTML = statCard('key','Total API Keys', s.totalApiKeys||0, null, 'primary') + statCard('speed','Active Keys', s.activeKeys||0, s.maxActiveKeys||20, 'info') + statCard('bar_chart','Total Requests', st.total||0, null, 'warning') + statCard('public','Proxy Pool Size', s.candidatesCount||0, null, 'success');
    const ws = $('warpStatus');
    if(ws) ws.innerHTML = `<span class="w-2 h-2 rounded-full ${s.warpAvailable?'bg-success':'bg-muted'}"></span> ${esc(s.warpStatus||'unknown')}`;
    const pools = s.pools || [];
    const ksc = $('keySlotCount'); if(ksc) ksc.textContent = pools.length + ' active key(s)';
    const ksw = $('keySlotWrap');
    if(ksw) {
      if(pools.length) {
        ksw.innerHTML = renderTable(['Key','Name','Slots','Proxy Address','Latency','Quality','Requests','Last Used'],
          pools.map(p => statRow([
            `<code class="text-mono-md px-1.5 py-0.5 bg-surface-container-high rounded">${esc(p.key)}</code>`, esc(p.name||''),
            badge((p.slots?p.slots.length:0)+' / 3', (p.slots&&p.slots.length)?'active':'pending'),
            `<span class="text-sm">${p.slots&&p.slots.length ? p.slots.map(sl => `<code class="text-mono-md px-1.5 py-0.5 bg-surface-container-high rounded">${esc(sl.addr)}</code>`).join('<br>') : '<span class="text-secondary">None</span>'}</span>`,
            p.slots&&p.slots.length ? p.slots.map(sl => badge(sl.latencyMs+'ms', sl.latencyMs<500?'green':sl.latencyMs<2000?'amber':'red')).join('<br>') : '-',
            p.slots&&p.slots.length ? p.slots.map(sl => badge(sl.grade||'C', sl.grade==='S'?'green':sl.grade==='A'?'blue':'gray')).join('<br>') : '-',
            p.requestCount||0, p.lastUsedAt ? new Date(p.lastUsedAt).toLocaleString('en-US') : 'Never'
          ]))
        );
      } else {
        ksw.innerHTML = `<div class="text-center py-12"><span class="material-symbols-outlined text-5xl text-secondary/30 block mb-3">lan</span><p class="text-body-md text-secondary">No active keys</p></div>`;
      }
    }
    fetchModels(); fetchDashboardKeys();
  } catch(e) { const sc = $('statCards'); if(sc) sc.innerHTML = skeletonCards(4); }
}

async function fetchModels() {
  try {
    const s = await api('/api/models');
    const el = $('testModel'); if(!el) return;
    const prev = el.value || localStorage.getItem('lastTestModel') || '';
    if (!s.models || !s.models.length) { el.innerHTML = '<option>No models available</option>'; return; }
    // Models the gateway has not confirmed callable are still listed, but
    // disabled and annotated — the gateway reroutes them, so a caller picking
    // one would silently get a different model back.
    const callable = new Set(Array.isArray(s.callableIds) && s.callableIds.length ? s.callableIds : s.models.map(m => m.id));
    const withheld = new Map((s.withheld || []).map(w => [w.id, w]));
    el.innerHTML = s.models.map(m => {
      if (callable.has(m.id)) return `<option value="${esc(m.id)}">${esc(m.id)}</option>`;
      const w = withheld.get(m.id) || {};
      const why = w.status ? ` — unavailable (HTTP ${esc(String(w.status))})` : ' — unverified';
      return `<option value="${esc(m.id)}" disabled>${esc(m.id)}${esc(why)}</option>`;
    }).join('');
    // A previously selected model may now be disabled; move to a working one.
    if (!el.value || el.options[el.selectedIndex]?.disabled) {
      const firstWorking = [...el.options].find(o => !o.disabled);
      if (firstWorking) el.value = firstWorking.value;
    }
    if (prev && s.models.some(m => m.id === prev) && callable.has(prev)) el.value = prev;
    localStorage.setItem('lastTestModel', el.value || '');
    el.addEventListener('change', () => localStorage.setItem('lastTestModel', el.value));
  } catch(_) { const el = $('testModel'); if(el) el.innerHTML = '<option>Failed to load</option>'; }
}

async function fetchDashboardKeys() {
  try {
    const s = await api('/api/keys');
    const k = s.keys || [];
    const enabled = k.filter(x => x.enabled !== false);
    const el = $('testKeySelect'); if(!el) return;
    el.innerHTML = enabled.length ? enabled.map((x,i) => `<option value="${esc(x.fullKey||x.key)}"${i===0?' selected':''}>${esc(x.name||x.key.slice(0,8))}</option>`).join('') : '<option value="">No keys available</option>';
  } catch(_) {}
}

async function sendTest() {
  const btn = $('testBtn'), mod = $('testModel'), msg = $('testPrompt'), key = $('testKeySelect'), streamCb = $('testStream');
  if (!btn||!mod||!msg||!key) return;
  if (!msg.value) { toast('Please enter a message','err'); return; }
  btn.innerHTML = '<span class="material-symbols-outlined text-lg animate-spin">sync</span> Sending...'; btn.disabled = true;
  const res = $('testResult');
  if (!res) return;
  res.className = 'mt-3 p-3 rounded-2xl bg-surface-container-low border border-border text-body-md max-h-96 overflow-y-auto font-mono whitespace-pre-wrap';
  res.innerHTML = '<span class="text-secondary animate-pulse">Waiting for response...</span>';

  const useStream = streamCb ? streamCb.checked : false;
  try {
    const h = {'Content-Type':'application/json'};
    if(key.value) h['Authorization'] = 'Bearer '+key.value;

    if (useStream) {
      // === Streaming Mode ===
      const resp = await fetch('/openai/v1/chat/completions', {
        method: 'POST',
        headers: h,
        body: JSON.stringify({model:mod.value,messages:[{role:'user',content:msg.value}],stream:true})
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => ({}));
        throw new Error(errData.error?.message || `HTTP ${resp.status}`);
      }
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let content = '';
      let usageInfo = '';
      res.innerHTML = '';
      const outDiv = res;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const data = line.slice(6).trim();
          if (data === '[DONE]') continue;
          try {
            const chunk = JSON.parse(data);
            const delta = chunk.choices?.[0]?.delta?.content;
            if (delta) content += delta;
            if (chunk.usage) {
              const u = chunk.usage;
              usageInfo = `\n\n—— End ——\nTokens: ${u.total_tokens || u.totalTokens || '?'} (prompt: ${u.prompt_tokens || u.promptTokens || '?'}, completion: ${u.completion_tokens || u.completionTokens || '?'})`;
            }
            outDiv.innerHTML = esc(content) + (usageInfo ? `<span class="text-secondary/60 block mt-3 pt-2 border-t border-border text-label-md">${usageInfo}</span>` : '');
          } catch {}
        }
      }
      // Process any remaining buffer
      if (!usageInfo && content) {
        outDiv.innerHTML = esc(content) + `<span class="text-secondary/60 block mt-3 pt-2 border-t border-border text-label-md">—— Stream End ——</span>`;
      }
    } else {
      // === Non-Streaming Mode ===
      const r = await fetch('/openai/v1/chat/completions',{method:'POST',headers:h,body:JSON.stringify({model:mod.value,messages:[{role:'user',content:msg.value}],stream:false})});
      const d = await r.json();
      if(!r.ok) throw new Error(d.error&&d.error.message?d.error.message:JSON.stringify(d));
      const content = d.choices&&d.choices[0]?d.choices[0].message.content:JSON.stringify(d);
      const usage = d.usage?`Tokens: ${d.usage.total_tokens} (prompt: ${d.usage.prompt_tokens}, completion: ${d.usage.completion_tokens})`:'';
      res.innerHTML = `<span class="flex items-center gap-1 text-success mb-1"><span class="material-symbols-outlined text-lg">check_circle</span> Success</span>` + esc(content) + (usage ? `<span class="text-secondary/60 block mt-2 pt-2 border-t border-border text-label-md">${usage}</span>` : '');
    }
  } catch(e) {
    res.className = 'mt-3 p-3 rounded-2xl bg-error/5 border border-error/30 text-body-md whitespace-pre-wrap';
    res.innerHTML = `<span class="flex items-center gap-1 text-error mb-1"><span class="material-symbols-outlined text-lg">error</span> Error</span><code class="text-mono-md">${esc(e.message)}</code>`;
  }
  btn.innerHTML = '<span class="material-symbols-outlined text-lg">send</span> Send Test'; btn.disabled = false;
}

/* ===== AUDIT ===== */
async function fetchAudit() {
  try {
    const s = await api('/api/audit');
    const sum = s.summary || {};
    const ac = $('auditCards');
    if(ac) ac.innerHTML = statCard('bar_chart','Total Requests', sum.totalRequests||0, null, 'primary') + statCard('token','Total Tokens', sum.totalTokens||0, null, 'success') + statCard('data_usage','Prompt', sum.totalPrompt||0, 'Completion: '+(sum.totalCompletion||0), 'info') + `<div class="bg-surface rounded-xl border border-border p-5 ambient-shadow hover:shadow-md hover:border-primary/30 transition-all"><div class="flex items-center gap-3 mb-2"><div class="w-9 h-9 rounded-full bg-warning/10 text-warning flex items-center justify-center"><span class="material-symbols-outlined text-lg">cached</span></div><span class="text-label-md text-secondary">Cache Hit Rate</span></div><div class="flex items-end gap-1 mt-2"><span class="text-display-lg text-on-surface font-bold tracking-tight">${(sum.cacheHitRate||0)*100}</span><span class="text-title-md text-secondary mb-1">%</span></div></div>`;
    const at = $('auditTable');
    if(at) {
      const days = s.days || [];
      if(days.length) {
        at.innerHTML = renderTable(['Date','Requests','Tokens','Prompt','Completion','Cache Reads'], days.map(d => statRow([d.date||'-', d.requests||0, d.totalTokens||0, d.promptTokens||0, d.completionTokens||0, d.cacheRead||0])));
      } else {
        at.innerHTML = `<div class="text-center py-12"><span class="material-symbols-outlined text-5xl text-secondary/30 block mb-3">bar_chart</span><p class="text-body-md text-secondary">No call records found</p></div>`;
      }
    }
  } catch(e) { const ac = $('auditCards'); if(ac) ac.innerHTML = skeletonCards(4); }
}

/* ===== KEYS ===== */
let allKeys = [];
async function fetchKeys() {
  try { const s = await api('/api/keys'); allKeys = s.keys || []; renderKeys(allKeys); }
  catch(e) { const kc = $('keyCards'); if(kc) kc.innerHTML = skeletonCards(4); }
}
function filterKeys() {
  const q = ($('keySearch')||{}).value||'';
  const f = allKeys.filter(k => (k.name||'').toLowerCase().includes(q)||(k.key||'').toLowerCase().includes(q)||(k.fullKey||'').toLowerCase().includes(q));
  renderKeys(f, q);
}
function renderKeys(keys, q) {
  const total = keys.length, enabled = keys.filter(k => k.enabled!==false).length, disabled = total - enabled;
  const expired = keys.filter(k => k.expiresAt>0&&k.expiresAt<Date.now()).length;
  const kc = $('keyCards');
  if(kc) kc.innerHTML = statCard('vpn_key','Total Keys', total, null, 'primary') + statCard('check_circle','Enabled', enabled, null, 'success') + statCard('block','Disabled', disabled, null, 'red') + statCard('schedule','Expired', expired, null, 'warning');
  const kt = $('keyTable'); if(!kt) return;
  kt.innerHTML = renderTable(['Key','Name','Status','Concurrency','Total Requests','Total Tokens','Expires At','Actions'],
    keys.map(k => {
      const isExpired = k.expiresAt>0&&k.expiresAt<Date.now();
      const status = isExpired?'expired':(k.enabled!==false?'active':'disabled');
      const statusLabel = isExpired?'Expired':(k.enabled!==false?'Enabled':'Disabled');
      return statRow([
        `<code class="text-mono-md px-1.5 py-0.5 bg-surface-container-high rounded">${esc(k.fullKey||k.key)}</code>`, esc(k.name||'-'), badge(statusLabel, status), k.maxConcurrency||'-', k.totalRequests||0, k.totalTokens||0,
        k.expiresAt?new Date(k.expiresAt).toLocaleString('en-US'):'Never',
        `<div class="flex gap-1"><button onclick="editKey('${k.key}')" class="flex items-center justify-center w-8 h-8 rounded-2xl text-secondary hover:bg-surface-container-high hover:text-primary transition-colors cursor-pointer"><span class="material-symbols-outlined text-[18px]">edit</span></button>`+
        `<button onclick="toggleKey('${k.key}')" class="flex items-center justify-center w-8 h-8 rounded-2xl text-secondary hover:bg-surface-container-high ${k.enabled!==false?'hover:text-warning':'hover:text-success'} transition-colors cursor-pointer"><span class="material-symbols-outlined text-[18px]">${k.enabled!==false?'pause':'play_arrow'}</span></button>`+
        `<button onclick="deleteKey('${k.key}')" class="flex items-center justify-center w-8 h-8 rounded-2xl text-secondary hover:bg-surface-container-high hover:text-error transition-colors cursor-pointer"><span class="material-symbols-outlined text-[18px]">delete</span></button></div>`
      ]);
    }), q?'No keys matching "'+esc(q)+'"':'No keys found. Click "Create Key" above to add one.'
  );
}

function showCreateKey() {
  showModal(`<h3 class="text-headline-sm font-bold text-on-surface mb-5 flex items-center gap-2"><span class="material-symbols-outlined text-primary">add_circle</span> Create New Key</h3>
    <div class="space-y-4">
      <div><label class="text-label-md text-secondary block mb-1.5">Name</label><input id="newKeyName" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none" placeholder="e.g., Development Key"/></div>
      <div><label class="text-label-md text-secondary block mb-1.5">Concurrency Limit</label><input id="newKeyConcurrency" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none" type="number" value="3"/></div>
      <div><label class="text-label-md text-secondary block mb-1.5">Max Requests</label><input id="newKeyMaxReqs" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none" type="number" placeholder="0 means unlimited"/></div>
      <div><label class="text-label-md text-secondary block mb-1.5">Expiration Time</label><input id="newKeyExpires" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none" type="datetime-local"/></div>
    </div>
    <div class="flex gap-3 mt-6">
      <button onclick="createKey()" class="flex-1 flex items-center justify-center gap-2 bg-primary text-on-primary px-4 py-2.5 rounded-2xl text-label-md hover:bg-primary-container transition-colors shadow-sm active:scale-[0.98] cursor-pointer"><span class="material-symbols-outlined text-lg">add</span> Create</button>
      <button onclick="closeModal()" class="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 bg-surface border border-border text-secondary rounded-2xl text-label-md hover:border-primary hover:text-primary transition-colors cursor-pointer">Cancel</button>
    </div>`);
}
async function createKey() {
  const name = ($('newKeyName')||{}).value||'';
  const concurrency = parseInt(($('newKeyConcurrency')||{}).value)||3;
  const maxReqs = ($('newKeyMaxReqs')||{}).value ? parseInt($('newKeyMaxReqs').value) : 0;
  const expires = ($('newKeyExpires')||{}).value ? new Date($('newKeyExpires').value).getTime() : 0;
  try { await api('/api/keys','POST',{name, maxConcurrency:concurrency, maxRequests:maxReqs, expiresAt:expires}); toast('Key created successfully','ok'); closeModal(); fetchKeys(); }
  catch(e) { toast('Creation failed: '+e.message,'err'); }
}
function editKey(keyId) {
  const k = allKeys.find(x => x.key === keyId); if(!k) return;
  showModal(`<h3 class="text-headline-sm font-bold text-on-surface mb-5 flex items-center gap-2"><span class="material-symbols-outlined text-primary">edit</span> Edit Key</h3>
    <div class="space-y-4">
      <div><label class="text-label-md text-secondary block mb-1.5">Key</label><code class="block px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-mono-md">${esc(k.fullKey||k.key)}</code></div>
      <div><label class="text-label-md text-secondary block mb-1.5">Name</label><input id="editKeyName" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none" value="${esc(k.name||'')}"/></div>
      <div><label class="flex items-center gap-2 cursor-pointer"><input id="editKeyEnabled" type="checkbox" class="w-4 h-4 rounded border-border text-primary focus:ring-primary/20" ${k.enabled!==false?'checked':''}/><span class="text-body-md text-secondary">Enable this key</span></label></div>
      <div><label class="text-label-md text-secondary block mb-1.5">Concurrency Limit</label><input id="editKeyConcurrency" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none" type="number" value="${k.maxConcurrency||3}"/></div>
      <div><label class="text-label-md text-secondary block mb-1.5">Max Requests</label><input id="editKeyMaxReqs" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none" type="number" value="${k.maxRequests||0}" placeholder="0 means unlimited"/></div>
    </div>
    <div class="flex gap-3 mt-6">
      <button onclick="doEditKey('${keyId}')" class="flex-1 flex items-center justify-center gap-2 bg-primary text-on-primary px-4 py-2.5 rounded-2xl text-label-md hover:bg-primary-container transition-colors shadow-sm active:scale-[0.98] cursor-pointer"><span class="material-symbols-outlined text-lg">save</span> Save</button>
      <button onclick="closeModal()" class="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 bg-surface border border-border text-secondary rounded-2xl text-label-md hover:border-primary hover:text-primary transition-colors cursor-pointer">Cancel</button>
    </div>`);
}
async function doEditKey(keyId) {
  const name = ($('editKeyName')||{}).value||'';
  const enabled = ($('editKeyEnabled')||{}).checked !== false;
  const concurrency = parseInt(($('editKeyConcurrency')||{}).value)||3;
  const maxReqs = ($('editKeyMaxReqs')||{}).value ? parseInt($('editKeyMaxReqs').value) : 0;
  try { await api('/api/keys/'+keyId,'PUT',{name, enabled, maxConcurrency:concurrency, maxRequests:maxReqs}); toast('Key updated successfully','ok'); closeModal(); fetchKeys(); }
  catch(e) { toast('Update failed: '+e.message,'err'); }
}
async function toggleKey(keyId) {
  try {
    const k = allKeys.find(x => x.key === keyId);
    await api('/api/keys/'+keyId,'PUT',{enabled: !k || k.enabled === false});
    toast('Key status toggled','ok'); fetchKeys();
  } catch(e) { toast('Operation failed: '+e.message,'err'); }
}
async function deleteKey(keyId) {
  if(!confirm('Are you sure you want to delete this key? This action cannot be undone.')) return;
  try { await api('/api/keys/'+keyId,'DELETE'); toast('Key deleted','ok'); fetchKeys(); }
  catch(e) { toast('Deletion failed: '+e.message,'err'); }
}

/* ===== PROXIES ===== */
async function fetchProxies() {
  startRefresh('proxies', fetchProxies, 15000);
  try {
    const s = await api('/api/proxies');
    const list = s.proxies || [];
    const pc = $('proxyCards');
    if(pc) pc.innerHTML = statCard('router','Total Proxies', s.count||0, null, 'primary') + statCard('check_circle','Locked', list.filter(x=>x.lockedBy).length||0, null, 'success') + statCard('error','Idle', list.filter(x=>!x.lockedBy).length||0, null, 'info') + statCard('electrical_services','Standby Pool', s.releasedCount||0, null, 'warning');
    const pt = $('proxyTable');
    if(pt) {
      if(list.length) {
        pt.innerHTML = renderTable(['Address','Protocol','Quality','Latency','Status','Actions'],
          list.slice(0,100).map(p => statRow([
            `<code class="text-mono-md">${esc(p.address)}</code>`, esc(p.protocol||'http'),
            badge(p.quality_grade||'C', p.quality_grade==='S'?'green':p.quality_grade==='A'?'blue':'gray'),
            (p.latency||'-')+'ms', p.lockedBy ? badge('Locked','active') : badge('Idle','gray'),
            `<button onclick="promoteProxy('${esc(p.address)}')" class="flex items-center justify-center w-8 h-8 rounded-2xl text-secondary hover:bg-surface-container-high hover:text-primary transition-colors cursor-pointer" title="Promote Priority"><span class="material-symbols-outlined text-[18px]">arrow_upward</span></button>`
          ]))
        );
      } else {
        pt.innerHTML = `<div class="text-center py-12"><span class="material-symbols-outlined text-5xl text-secondary/30 block mb-3">router</span><p class="text-body-md text-secondary">No proxy data available</p></div>`;
      }
    }
  } catch(e) { const pc = $('proxyCards'); if(pc) pc.innerHTML = skeletonCards(4); }
}
async function promoteProxy(addr) {
  try { await api('/api/promote','POST',{addr}); toast('Proxy priority promoted','ok'); fetchProxies(); }
  catch(e) { toast('Operation failed: '+e.message,'err'); }
}

function showImportProxies() {
  showModal(`<h3 class="text-headline-sm font-bold text-on-surface mb-5 flex items-center gap-2"><span class="material-symbols-outlined text-primary">playlist_add</span> Batch Import Proxies</h3>
    <p class="text-body-md text-secondary mb-4">One proxy address per line. Supported formats: <code class="text-mono-md bg-surface-container-low px-2 py-0.5 rounded-lg">http://host:port</code> <code class="text-mono-md bg-surface-container-low px-2 py-0.5 rounded-lg">socks5://host:port</code></p>
    <textarea id="importProxyText" class="w-full h-44 px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-mono-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none resize-y" placeholder="socks5://1.2.3.4:1080&#10;http://5.6.7.8:3128&#10;socks5://9.10.11.12:1080"></textarea>
    <p id="importProxyStatus" class="text-label-md text-secondary mt-2 hidden"></p>
    <div class="flex gap-3 mt-5">
      <button onclick="importProxies()" id="importProxyBtn" class="flex-1 flex items-center justify-center gap-2 bg-primary text-on-primary px-4 py-2.5 rounded-2xl text-label-md hover:bg-primary-container transition-colors shadow-sm cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"><span class="material-symbols-outlined text-lg">upload</span> Import</button>
      <button onclick="closeModal()" class="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 bg-surface border border-border text-secondary rounded-2xl text-label-md hover:border-primary hover:text-primary transition-colors cursor-pointer">Cancel</button>
    </div>`);
}

async function importProxies() {
  const btn = $('importProxyBtn');
  const ta = $('importProxyText');
  const st = $('importProxyStatus');
  if (!btn || !ta) return;
  const lines = ta.value.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#'));
  if (!lines.length) { toast('Please enter proxy addresses','err'); return; }
  btn.disabled = true;
  btn.innerHTML = '<span class="material-symbols-outlined text-lg animate-spin">sync</span> Importing...';
  if (st) { st.className = 'text-label-md text-secondary mt-2'; st.textContent = ''; }
  try {
    // Import in batches of 50
    const batchSize = 50;
    let total = 0;
    for (let i = 0; i < lines.length; i += batchSize) {
      const batch = lines.slice(i, i + batchSize);
      const r = await api('/api/proxies', 'POST', { proxies: batch });
      total += r.count || 0;
      if (st) st.textContent = `Progress: ${Math.min(i+batchSize, lines.length)}/${lines.length} (Added ${total})`;
    }
    if (st) st.className = 'text-label-md text-success mt-2';
    toast(`Import complete: ${total} proxies added`,'ok');
    closeModal();
    fetchProxies();
  } catch(e) {
    toast('Import failed: '+e.message,'err');
    if (st) { st.className = 'text-label-md text-error mt-2'; st.textContent = 'Failed: '+e.message; }
  }
  btn.disabled = false;
  btn.innerHTML = '<span class="material-symbols-outlined text-lg">upload</span> Import';
}

/* ===== SOURCES ===== */
async function fetchSources() {
  startRefresh('sources', fetchSources, 15000);
  try {
    const s = await api('/api/sources');
    const list = s.sources || [];
    const st = $('sourceTable');
    if(!st) return;
    if(list.length) {
      st.innerHTML = renderTable(['Name','Type','Proxy Count','Status','Actions'],
        list.map(src => statRow([
          esc(src.name||''),
          esc(src.type||'scraper'),
          src.count||0,
          src.error
            ? `<span title="${esc(src.error)}">${badge('Error','red')}</span>`
            : badge('Healthy','green'),
          `<button onclick="deleteSource('${esc(src.name)}')" class="flex items-center justify-center w-8 h-8 rounded-2xl text-secondary hover:bg-error/10 hover:text-error transition-colors cursor-pointer" title="Delete"><span class="material-symbols-outlined text-[18px]">delete</span></button>`
        ]))
      );
    } else {
      st.innerHTML = `<div class="text-center py-12"><span class="material-symbols-outlined text-5xl text-secondary/30 block mb-3">source</span><p class="text-body-md text-secondary">No proxy sources available. Click "Add Source" above to get started.</p></div>`;
    }
  } catch(e) { const st = $('sourceTable'); if(st) st.innerHTML = `<div class="text-center py-12"><p class="text-error">Failed to load: ${esc(e.message)}</p></div>`; }
}

function showAddSource() {
  showModal(`<h3 class="text-headline-sm font-bold text-on-surface mb-5 flex items-center gap-2"><span class="material-symbols-outlined text-primary">add_circle</span> Add Proxy Source</h3>
    <div class="space-y-4">
      <div>
        <label class="text-label-md text-secondary block mb-1.5" for="srcName">Name</label>
        <input id="srcName" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none" placeholder="e.g., my-proxy-source"/>
      </div>
      <div>
        <label class="text-label-md text-secondary block mb-1.5" for="srcUrl">URL</label>
        <input id="srcUrl" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none font-mono text-mono-md" placeholder="https://example.com/proxies.txt"/>
      </div>
      <div>
        <label class="text-label-md text-secondary block mb-1.5" for="srcType">Type</label>
        <select id="srcType" class="w-full px-3 py-2 bg-surface-container-low border border-border rounded-2xl text-body-md focus:border-primary focus:ring-2 focus:ring-primary/20 transition-all outline-none">
          <option value="text">text (plain text, one per line)</option>
          <option value="json">json (JSON array format)</option>
          <option value="scraper">scraper (web page scrape)</option>
        </select>
      </div>
    </div>
    <div class="flex gap-3 mt-6">
      <button onclick="addSource()" id="addSrcBtn" class="flex-1 flex items-center justify-center gap-2 bg-primary text-on-primary px-4 py-2.5 rounded-2xl text-label-md hover:bg-primary-container transition-colors shadow-sm cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"><span class="material-symbols-outlined text-lg">add</span> Add</button>
      <button onclick="closeModal()" class="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 bg-surface border border-border text-secondary rounded-2xl text-label-md hover:border-primary hover:text-primary transition-colors cursor-pointer">Cancel</button>
    </div>`);
}

async function addSource() {
  const btn = $('addSrcBtn');
  const name = $('srcName')?.value.trim();
  const url = $('srcUrl')?.value.trim();
  const type = $('srcType')?.value || 'text';
  if (!btn || !name || !url) { toast('Name and URL are required','err'); return; }
  btn.disabled = true;
  btn.innerHTML = '<span class="material-symbols-outlined text-lg animate-spin">sync</span> Adding...';
  try {
    await api('/api/sources', 'POST', { name, url, type });
    toast(`Proxy source "${name}" added`,'ok');
    closeModal();
    fetchSources();
  } catch(e) {
    toast('Failed to add: '+e.message,'err');
  }
  btn.disabled = false;
  btn.innerHTML = '<span class="material-symbols-outlined text-lg">add</span> Add';
}

async function deleteSource(name) {
  if (!confirm(`Are you sure you want to delete proxy source "${name}"?`)) return;
  try {
    await api('/api/sources/'+encodeURIComponent(name), 'DELETE');
    toast(`Proxy source "${name}" deleted`,'ok');
    fetchSources();
  } catch(e) {
    toast('Deletion failed: '+e.message,'err');
  }
}

/* ===== CONFIG ===== */
async function fetchConfig() {
  try {
    const s = await api('/api/config');
    const cfg = $('cfgPort'); if(cfg) cfg.value = s.port||'13339';
    const cc = $('cfgConcurrency'); if(cc) cc.value = s.maxActiveKeys||20;
    const cs = $('cfgSlotCount'); if(cs) cs.value = s.slotCount||3;
    const cw = $('cfgWarpMode'); if(cw) cw.value = s.warpMode||'off';
    const cf = $('cfgFallback'); if(cf) cf.value = s.fallbackProxy||'';
    const cr = $('cfgRefreshMs'); if(cr) cr.value = s.proxyRefreshMs||300000;
    toast('Configuration loaded','ok');
  } catch(e) { toast('Failed to load configuration: '+e.message,'err'); }
}
async function saveConfig() {
  const btn = $('cfgSaveBtn');
  if (!btn) return;
  btn.disabled = true;
  btn.innerHTML = '<span class="material-symbols-outlined animate-spin text-[18px]">sync</span> Saving...';
  try {
    const port = parseInt(($('cfgPort')||{}).value);
    const maxActiveKeys = parseInt(($('cfgConcurrency')||{}).value);
    const slotCount = parseInt(($('cfgSlotCount')||{}).value);
    const refreshMs = parseInt(($('cfgRefreshMs')||{}).value);
    const warpMode = ($('cfgWarpMode')||{}).value;
    const fallback = ($('cfgFallback')||{}).value;
    // Client-side validation
    const errors = [];
    if (!port || port < 1 || port > 65535) errors.push('Port must be between 1 and 65535');
    if (!maxActiveKeys || maxActiveKeys < 1 || maxActiveKeys > 100) errors.push('Active keys limit must be between 1 and 100');
    if (!slotCount || slotCount < 1 || slotCount > 20) errors.push('Slot count must be between 1 and 20');
    if (!refreshMs || refreshMs < 5000 || refreshMs > 600000) errors.push('Refresh interval must be between 5000 and 600000ms');
    if (fallback && !/^socks5:\/\/.+/.test(fallback)) errors.push('Fallback format must be: socks5://host:port');
    if (errors.length) {
      toast('Validation failed: ' + errors.join('; '), 'err');
      btn.disabled = false;
      btn.innerHTML = '<span class="material-symbols-outlined text-[18px]">save</span> Save Config';
      return;
    }
    const body = { port, maxActiveKeys, slotCount, warpMode, fallbackProxy: fallback, proxyRefreshMs: refreshMs };
    await api('/api/config','POST', body);
    toast('Configuration saved','ok');
    fetchConfig();
  } catch(e) {
    toast('Failed to save: '+e.message,'err');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<span class="material-symbols-outlined text-[18px]">save</span> Save Config';
  }
}

/* ===== LOGS ===== */
async function fetchLogs() {
  startRefresh('logs', fetchLogs, 10000);
  try {
    const s = await api('/api/logs');
    const lb = $('logBox'); if(!lb) return;
    lb.textContent = (s.logs||[]).join('\n')||'No logs available';
    lb.scrollTop = lb.scrollHeight;
  } catch(e) { const lb = $('logBox'); if(lb) lb.textContent = 'Failed to load logs: '+e.message; }
}

/* ===== INIT ===== */
setTimeout(() => { fetchProxies(); fetchSources(); }, 200);

const PLATFORM_COLORS = {
  instagram: { border: '#E1306C', bg: '#E1306C33' },
  facebook:  { border: '#1877F2', bg: '#1877F233' },
  reddit:    { border: '#FF4500', bg: '#FF450033' },
};
const SENTIMENT_COLORS = { positive: '#2e9e5f', neutral: '#888888', negative: '#d64545' };
const DATA_BASE = 'data';

const state = { index: null, dayCache: new Map(), from: null, to: null, charts: {} };

async function fetchJSON(url) {
  const r = await fetch(url + '?t=' + Date.now());
  if (!r.ok) throw new Error(url + ' -> ' + r.status);
  return r.json();
}

function ymd(d) { return d.toISOString().slice(0, 10); }

function applyPreset(val) {
  const today = new Date();
  const from = val === 'all' ? new Date('2000-01-01') : new Date(today);
  if (val !== 'all') from.setDate(from.getDate() - parseInt(val, 10));
  state.from = ymd(from);
  state.to = ymd(today);
}

function inRange(day) { return day >= state.from && day <= state.to; }

function escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, m => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[m]));
}

function fmtRelative(iso) {
  if (!iso) return '';
  const then = new Date(iso);
  const mins = Math.floor((Date.now() - then) / 60000);
  if (mins < 60) return `updated ${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `updated ${hrs}h ago`;
  return `updated ${Math.floor(hrs / 24)}d ago`;
}

async function loadDay(platform, day) {
  const key = `${platform}/${day}`;
  if (state.dayCache.has(key)) return state.dayCache.get(key);
  try {
    const data = await fetchJSON(`${DATA_BASE}/${platform}/${day}.json`);
    state.dayCache.set(key, data);
    return data;
  } catch {
    state.dayCache.set(key, []);
    return [];
  }
}

async function loadRange() {
  const out = [];
  for (const p of state.index.platforms) {
    for (const day of state.index.days_by_platform[p] || []) {
      if (inRange(day)) {
        const items = await loadDay(p, day);
        for (const it of items) out.push({ ...it, _day: day });
      }
    }
  }
  return out;
}

function drawKpis(items) {
  const english = items.filter(i => i.language === 'english' && i.sentiment);
  const pos = english.filter(i => i.sentiment === 'positive').length;
  const neg = english.filter(i => i.sentiment === 'negative').length;
  const total = items.length;
  document.getElementById('kpiTotal').textContent = total;
  document.getElementById('kpiPos').textContent = pos;
  document.getElementById('kpiNeg').textContent = neg;
  document.getElementById('kpiPosPct').textContent = english.length ? `${Math.round(pos/english.length*100)}% of english` : '—';
  document.getElementById('kpiNegPct').textContent = english.length ? `${Math.round(neg/english.length*100)}% of english` : '—';
  const active = new Set(items.map(i => i.platform));
  document.getElementById('kpiPlatforms').textContent = active.size || state.index.platforms.length;
}

function drawVolume() {
  const rows = state.index.volume_by_day.filter(r => inRange(r.date));
  const labels = rows.map(r => r.date);
  const datasets = state.index.platforms.map(p => ({
    label: p.charAt(0).toUpperCase() + p.slice(1),
    data: rows.map(r => r[p] || 0),
    borderColor: PLATFORM_COLORS[p]?.border || '#666',
    backgroundColor: PLATFORM_COLORS[p]?.bg || '#66666633',
    fill: true, tension: 0.3, borderWidth: 2, pointRadius: 2,
  }));
  state.charts.volume?.destroy();
  state.charts.volume = new Chart(document.getElementById('volumeChart'), {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { usePointStyle: true, boxWidth: 8 } } },
      scales: { y: { stacked: true, beginAtZero: true, grid: { color: 'rgba(128,128,128,0.1)' } },
                x: { stacked: true, grid: { display: false } } }
    }
  });
}

function drawSentiment() {
  const sbp = state.index.sentiment_by_platform || {};
  const classes = ['positive', 'neutral', 'negative'];
  const datasets = classes.map(c => ({
    label: c,
    backgroundColor: SENTIMENT_COLORS[c],
    data: state.index.platforms.map(p => (sbp[p] || {})[c] || 0),
    borderRadius: 4,
  }));
  state.charts.sent?.destroy();
  state.charts.sent = new Chart(document.getElementById('sentimentChart'), {
    type: 'bar',
    data: { labels: state.index.platforms.map(p => p[0].toUpperCase() + p.slice(1)), datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { usePointStyle: true, boxWidth: 8 } } },
      scales: { x: { stacked: true, grid: { display: false } },
                y: { stacked: true, beginAtZero: true, grid: { color: 'rgba(128,128,128,0.1)' } } }
    }
  });
}

function drawTopics() {
  const t = state.index.predefined_topics || {};
  const labels = Object.keys(t);
  const data = labels.map(k => t[k]);
  state.charts.topic?.destroy();
  state.charts.topic = new Chart(document.getElementById('topicChart'), {
    type: 'bar',
    data: { labels, datasets: [{ label: 'Comments', data, backgroundColor: '#4a6fa5', borderRadius: 4 }] },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y',
      plugins: { legend: { display: false } },
      scales: { x: { beginAtZero: true, grid: { color: 'rgba(128,128,128,0.1)' } },
                y: { grid: { display: false } } }
    }
  });
}

function fillAutoThemes() {
  const tb = document.querySelector('#autoThemes tbody');
  if (!tb) return;
  tb.innerHTML = '';
  (state.index.auto_themes || []).slice(0, 15).forEach(t => {
    const tr = document.createElement('tr');
    tr.innerHTML =
      `<td><strong>${escapeHtml(t.theme)}</strong></td>` +
      `<td>${t.count}</td>` +
      `<td class="muted">${escapeHtml((t.example || '').slice(0, 120))}</td>`;
    tb.appendChild(tr);
  });
  if (!tb.children.length) tb.innerHTML = '<tr><td colspan="3" class="muted">No themes yet.</td></tr>';
}

function fillCommenters() {
  const tb = document.querySelector('#topCommenters tbody');
  if (!tb) return;
  tb.innerHTML = '';
  (state.index.top_commenters || []).slice(0, 15).forEach(c => {
    const tr = document.createElement('tr');
    tr.innerHTML =
      `<td>${escapeHtml(c.author)}</td>` +
      `<td><span class="pill ${c.platform}">${c.platform}</span></td>` +
      `<td>${c.count}</td>`;
    tb.appendChild(tr);
  });
  if (!tb.children.length) tb.innerHTML = '<tr><td colspan="3" class="muted">No data yet.</td></tr>';
}

function fillSubredditPanel() {
  const tb = document.querySelector('#subredditPanel tbody');
  if (!tb) return;
  tb.innerHTML = '';
  const rows = (state.index && state.index.reddit_subreddits) || [];
  if (!rows.length) {
    tb.innerHTML = '<tr><td colspan="3" class="muted">No Reddit mentions yet.</td></tr>';
  } else {
    rows.forEach(r => {
      const tr = document.createElement('tr');
      const sent = r.dominant_sentiment ? `<span class="pill ${r.dominant_sentiment}">${r.dominant_sentiment}</span>` : '—';
      tr.innerHTML = `<td>r/${escapeHtml(r.subreddit)}</td><td>${r.count}</td><td>${sent}</td>`;
      tb.appendChild(tr);
    });
  }
  const banner = document.getElementById('redditUncertain');
  if (banner) {
    const n = (state.index && state.index.reddit_uncertain_count) || 0;
    banner.innerHTML = n ? `<div class="banner">⚠ ${n} Reddit match(es) flagged as uncertain by the filter.</div>` : '';
  }
}

async function fillTopComments(items) {
  const english = items.filter(i => i.language === 'english' && i.sentiment);
  const render = (selector, list) => {
    const ul = document.querySelector(selector);
    if (!ul) return;
    ul.innerHTML = '';
    if (!list.length) { ul.innerHTML = '<li class="muted">No comments in range.</li>'; return; }
    list.slice(0, 8).forEach(it => {
      const li = document.createElement('li');
      const sub = it.platform === 'reddit' && it.reddit_subreddit ? ` · r/${escapeHtml(it.reddit_subreddit)}` : '';
      const link = it.post_url ? ` <a href="${it.post_url}" target="_blank" rel="noopener">open ↗</a>` : '';
      li.innerHTML =
        `<span class="pill ${it.platform}">${it.platform}</span>` +
        ` <span class="muted">${escapeHtml(it._day)}${sub}</span><br/>` +
        `${escapeHtml((it.text || '').slice(0, 240))}${link}`;
      ul.appendChild(li);
    });
  };
  render('#topPositive', english.filter(i => i.sentiment === 'positive')
    .sort((a, b) => (b.like_count || 0) - (a.like_count || 0)));
  render('#topNegative', english.filter(i => i.sentiment === 'negative')
    .sort((a, b) => (b.like_count || 0) - (a.like_count || 0)));
}

const STOPWORDS = new Set(('the a an and or but is are was were be been being have has had do does did will '
  + 'would could should may might must shall can to of in on at by for with about against between into '
  + 'through during before after above below from up down out off over under again further then once here '
  + 'there when where why how all any both each few more most other some such no nor not only own same so '
  + 'than too very s t just don now i me my we our you your he him his she her it its they them their what '
  + 'which who whom this that these those am if as because while u ok yes like also get got one two im ur '
  + 'aza fashions').split(' '));

function tokenize(text) {
  return (text || '')
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, ' ')
    .replace(/[^a-z\u0900-\u097F\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length >= 3 && !STOPWORDS.has(w));
}

function wordFreq(items) {
  const counts = new Map();
  for (const it of items) for (const tok of tokenize(it.text)) counts.set(tok, (counts.get(tok) || 0) + 1);
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 60);
}

function drawWordClouds(items) {
  if (typeof WordCloud !== 'function') return;
  const by = { positive: [], negative: [], neutral: [] };
  for (const it of items) if (by[it.sentiment]) by[it.sentiment].push(it);
  const paint = (id, color, list) => {
    const el = document.getElementById(id);
    if (!el || !list.length) return;
    const max = list[0][1];
    WordCloud(el, {
      list, gridSize: 6,
      weightFactor: size => 8 + (size / max) * 34,
      fontFamily: 'system-ui, sans-serif',
      color: () => color,
      backgroundColor: 'transparent',
      rotateRatio: 0.15, shuffle: true,
    });
  };
  paint('wordCloudPositive', SENTIMENT_COLORS.positive, wordFreq(by.positive));
  paint('wordCloudNegative', SENTIMENT_COLORS.negative, wordFreq(by.negative));
  paint('wordCloudNeutral',  SENTIMENT_COLORS.neutral,  wordFreq(by.neutral));
}

async function redraw() {
  const items = await loadRange();
  drawKpis(items);
  drawVolume();
  drawSentiment();
  drawTopics();
  fillAutoThemes();
  fillCommenters();
  fillSubredditPanel();
  await fillTopComments(items);
  drawWordClouds(items);
}

async function init() {
  try {
    state.index = await fetchJSON(`${DATA_BASE}/index.json`);
    applyPreset('30');
    const picker = document.getElementById('rangePreset');
    picker?.addEventListener('change', e => { applyPreset(e.target.value); redraw(); });
    document.getElementById('lastUpdated').textContent = fmtRelative(state.index.generated_at);
    await redraw();
  } catch (err) {
    document.getElementById('error').innerHTML =
      `<div class="card" style="border-color:var(--neg); color:var(--neg)">
        Failed to load data: ${escapeHtml(err.message)}. If this is a brand-new repo, wait for the first daily run to finish.
      </div>`;
  }
}

document.addEventListener('DOMContentLoaded', init);

const PLATFORM_COLORS = {
  instagram: { border: '#E1306C', bg: '#E1306C33' },
  facebook:  { border: '#1877F2', bg: '#1877F233' },
  reddit:    { border: '#FF4500', bg: '#FF450033' },
};

const DATA_BASE = 'data';

const state = {
  index: null,
  dayCache: new Map(),
  from: null,
  to: null,
  charts: {},
};

async function fetchJSON(url) {
  const r = await fetch(url + '?t=' + Date.now());
  if (!r.ok) throw new Error(url + ' -> ' + r.status);
  return r.json();
}

function ymd(d) { return d.toISOString().slice(0, 10); }

function applyPreset(val) {
  const today = new Date();
  let from;
  if (val === 'all') {
    from = new Date('2000-01-01');
  } else {
    from = new Date(today);
    from.setDate(from.getDate() - parseInt(val, 10));
  }
  state.from = ymd(from);
  state.to = ymd(today);
}

function inRange(day) { return day >= state.from && day <= state.to; }

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

function escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, m => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[m]));
}

function drawVolume() {
  const rows = state.index.volume_by_day.filter(r => inRange(r.date));
  const labels = rows.map(r => r.date);
  const datasets = state.index.platforms.map(p => ({
    label: p.charAt(0).toUpperCase() + p.slice(1),
    data: rows.map(r => r[p] || 0),
    borderColor: PLATFORM_COLORS[p]?.border || '#666',
    backgroundColor: PLATFORM_COLORS[p]?.bg || '#66666633',
    fill: true,
    tension: 0.2,
  }));
  state.charts.volume?.destroy();
  state.charts.volume = new Chart(document.getElementById('volumeChart'), {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      scales: { y: { stacked: true, beginAtZero: true }, x: { stacked: true } }
    }
  });
}

function drawSentiment() {
  const sbp = state.index.sentiment_by_platform || {};
  const classes = ['positive', 'neutral', 'negative'];
  const colors = { positive: '#2e9e5f', neutral: '#888', negative: '#d64545' };
  const datasets = classes.map(c => ({
    label: c,
    backgroundColor: colors[c],
    data: state.index.platforms.map(p => (sbp[p] || {})[c] || 0),
  }));
  state.charts.sent?.destroy();
  state.charts.sent = new Chart(document.getElementById('sentimentChart'), {
    type: 'bar',
    data: { labels: state.index.platforms, datasets },
    options: { responsive: true, scales: { y: { beginAtZero: true } } }
  });
}

function drawTopics() {
  const t = state.index.predefined_topics || {};
  const labels = Object.keys(t);
  const data = labels.map(k => t[k]);
  state.charts.topic?.destroy();
  state.charts.topic = new Chart(document.getElementById('topicChart'), {
    type: 'bar',
    data: { labels, datasets: [{ label: 'Comments', data, backgroundColor: '#4a6fa5' }] },
    options: { responsive: true, indexAxis: 'y', scales: { x: { beginAtZero: true } } }
  });
}

function fillAutoThemes() {
  const tb = document.querySelector('#autoThemes tbody');
  if (!tb) return;
  tb.innerHTML = '';
  (state.index.auto_themes || []).slice(0, 25).forEach(t => {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + escapeHtml(t.theme) + '</td>' +
      '<td>' + t.count + '</td>' +
      '<td>' + escapeHtml((t.example || '').slice(0, 140)) + '</td>';
    tb.appendChild(tr);
  });
}

function fillCommenters() {
  const tb = document.querySelector('#topCommenters tbody');
  if (!tb) return;
  tb.innerHTML = '';
  (state.index.top_commenters || []).slice(0, 25).forEach(c => {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + escapeHtml(c.author) + '</td>' +
      '<td>' + escapeHtml(c.platform) + '</td>' +
      '<td>' + c.count + '</td>';
    tb.appendChild(tr);
  });
}

function fillSubredditPanel() {
  const tb = document.querySelector('#subredditPanel tbody');
  if (!tb) return;
  tb.innerHTML = '';
  const rows = (state.index && state.index.reddit_subreddits) || [];
  if (rows.length === 0) {
    tb.innerHTML = '<tr><td colspan="3" class="muted">No Reddit mentions yet</td></tr>';
  } else {
    rows.forEach(r => {
      const tr = document.createElement('tr');
      tr.innerHTML =
        '<td>r/' + escapeHtml(r.subreddit) + '</td>' +
        '<td>' + r.count + '</td>' +
        '<td>' + escapeHtml(r.dominant_sentiment || '—') + '</td>';
      tb.appendChild(tr);
    });
  }
  const banner = document.getElementById('redditUncertain');
  if (banner) {
    const n = (state.index && state.index.reddit_uncertain_count) || 0;
    banner.textContent = n
      ? '⚠ ' + n + ' Reddit match(es) flagged as uncertain by the filter.'
      : '';
  }
}

async function fillTopComments() {
  const items = await loadRange();
  const english = items.filter(i => i.language === 'english' && i.sentiment);
  const renderList = (selector, list) => {
    const ul = document.querySelector(selector);
    if (!ul) return;
    ul.innerHTML = '';
    list.slice(0, 10).forEach(it => {
      const li = document.createElement('li');
      const sub = it.platform === 'reddit' && it.reddit_subreddit
        ? ' (r/' + escapeHtml(it.reddit_subreddit) + ')' : '';
      const link = it.post_url
        ? '<a href="' + it.post_url + '" target="_blank" rel="noopener">link</a>' : '';
      li.innerHTML =
        '<strong>' + escapeHtml(it.platform) + sub + '</strong> — ' +
        escapeHtml((it.text || '').slice(0, 240)) + ' ' + link;
      ul.appendChild(li);
    });
  };
  renderList('#topPositive', english.filter(i => i.sentiment === 'positive')
    .sort((a, b) => (b.like_count || 0) - (a.like_count || 0)));
  renderList('#topNegative', english.filter(i => i.sentiment === 'negative')
    .sort((a, b) => (b.like_count || 0) - (a.like_count || 0)));
}

async function redraw() {
  drawVolume();
  drawSentiment();
  drawTopics();
  fillAutoThemes();
  fillCommenters();
  fillSubredditPanel();
  await fillTopComments();
}

async function init() {
  try {
    state.index = await fetchJSON(`${DATA_BASE}/index.json`);
    applyPreset('30');
    const picker = document.getElementById('rangePreset');
    if (picker) {
      picker.addEventListener('change', e => {
        applyPreset(e.target.value);
        redraw();
      });
    }
    await redraw();
  } catch (err) {
    const root = document.getElementById('error') || document.body;
    root.insertAdjacentHTML(
      'beforeend',
      '<div style="padding:16px;color:#b00">Failed to load data: ' +
        escapeHtml(err.message) +
        '. If this is a brand-new repo, wait for the first daily run to finish.</div>'
    );
  }
}

document.addEventListener('DOMContentLoaded', init);

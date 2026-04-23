const PLATFORM_COLORS = {
  instagram: { border: '#E1306C', bg: '#E1306C33' },
  facebook:  { border: '#1877F2', bg: '#1877F233' },
  reddit:    { border: '#FF4500', bg: '#FF450033' },
};

const DATA_BASE = 'data'; // if dashboard lives in /dashboard. Change to 'data' if served at repo root.

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
  if (val === 'all') { from = new Date('2000-01-01'); }
  else { from = new Date(today); from.setDate(from.getDate() - parseInt(val, 10)); }
  state.from = ymd(from); state.to = ymd(today);
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

function drawVolume() {
  const rows = state.index.volume_by_day.filter(r => inRange(r.date));
  const labels = rows.map(r => r.date);
  const datasets = state.index.platforms.map(p => ({
    label: p.charAt(0).toUpperCase() + p.slice(1),
    data: rows.map(r => r[p] || 0),
    borderColor: PLATFORM_COLORS[p]?.border || '#666',
    backgroundColor: PLATFORM_COLORS[p]?.bg || '#66666633',
    fill: true, tension: 0.2,
  }));
  state.charts.volume?.destroy();
  state.charts.volume = new Chart(document.getElementById('volumeChart'), {
    type: 'line', data: { labels, datasets },
    options: { responsive: true, interaction: { mode: 'index', intersect: false },
               scales: { y: { stacked: true, beginAtZero: true }, x: { stacked: true } } }
  });
}

function drawSentiment() {
  const sbp = state.index.sentiment_by_platform;
  const classes = ['positive', 'neutral', 'negative'];
  const colors = { positive: '#2e9e5f', neutral: '#888', negative: '#d64545' };
  const datasets = classes.map(c => ({
    label: c, backgroundColor: colors[c],
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

function fillSubredditPanel() {
  const tb = document.querySelector('#subredditPanel tbody');
  if (!tb) return;
  tb.innerHTML = '';
  const rows = (state.index && state.index.reddit_subreddits) || [];
  if (rows.length === 0) {
    tb.innerHTML = '<tr><td colspan="3" class="muted">No Reddit mentions yet</td></tr>';
    return;
  }
  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>r/' + escapeHtml(r.subreddit) + '</td>' +
      '<td>' + r.count + '</td>' +
      '<td>' + (r.dominant_sentiment || '—') + '</td>';
    tb.appendChild(tr);
  });

  const banner = document.getElementById('redditUncertain');
  if (banner) {
    const n = (state.index && state.index.reddit_uncertain_count) || 0;
    banner.textContent = n
      ? '⚠ ' + n + ' Reddit match(es) flagged as uncertain by the filter.'
      : '';
  }
}

function fillAutoThemes() {
  const tb = document.querySelector('#autoThemes tbody');
  tb.innerHTML = '';
  (state.index.auto_themes || []).forEach(t => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${escapeHtml(t.theme)}</td><td>${t.count}</td><td>${escapeHtml(t.example)}</td>`;
    tb.appendChild(tr);
  });
}

function fillTopCommenters() {
  const tb = document.querySelector('#topCommenters tbody');
  tb.innerHTML = '';
  (state.index.top_commenters || []).forEach(c => {
  const tr = document.createElement('tr');
  const platformLabel = c.platform === 'reddit' ? 'reddit' : c.platform;
  tr.innerHTML = `<td>${escapeHtml(c.author)}</td><td>${platformLabel}</td><td>${c.count}</td>`;
  tb.appendChild(tr);
});

function escapeHtml(s) {
  return String(s || '').replace(/[&<>"']/g, m => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[m]));
}

async function fillCommentTables(all) {
  const neg = all.filter(x => x.sentiment === 'negative')
                 .sort((a,b) => b._day.localeCompare(a._day)).slice(0, 50);
  const pos = all.filter(x => x.sentiment === 'positive')
                 .sort((a,b) => b._day.localeCompare(a._day)).slice(0, 50);
  const render = (sel, rows) => {
    const tb = document.querySelector(sel + ' tbody');
    tb.innerHTML = '';
    rows.forEach(r => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${r._day}</td>
        <td>${r.platform}</td>
        <td>${escapeHtml(r.text)}</td>
        <td><a href="${r.post_url}" target="_blank" rel="noopener">open</a></td>`;
      tb.appendChild(tr);
    });
  };
  render('#topNegative', neg);
  render('#topPositive', pos);
}

function drawWordClouds(all) {
  const stop = new Set(('the a an and or but if then is are was were be been being of to in on for with at by from this that these those it its i you we they he she me my your our their not no yes so very really just').split(' '));
  const buckets = { positive: [], negative: [], neutral: [] };
  all.forEach(x => {
    if (x.language !== 'english' || !x.sentiment) return;
    const words = (x.text || '').toLowerCase().replace(/[^a-z\s']/g, ' ').split(/\s+/)
      .filter(w => w.length > 2 && !stop.has(w));
    words.forEach(w => buckets[x.sentiment] && buckets[x.sentiment].push(w));
  });
  const render = (id, words) => {
    const freq = {};
    words.forEach(w => freq[w] = (freq[w] || 0) + 1);
    const list = Object.entries(freq).sort((a,b) => b-a).slice(0, 120);[1]
    WordCloud(document.getElementById(id), {
      list, gridSize: 6, weightFactor: (s) => 6 + s * 2, rotateRatio: 0.2
    });
  };
  render('wcPositive', buckets.positive);
  render('wcNegative', buckets.negative);
  render('wcNeutral',  buckets.neutral);
}

async function redraw() {
  drawVolume(); drawSentiment(); drawTopics();
  fillAutoThemes(); fillTopCommenters();
  const all = await loadRange();
  await fillCommentTables(all);
  drawWordClouds(all);
}

async function init() {
  state.index = await fetchJSON(`${DATA_BASE}/index.json`);
  document.getElementById('generatedAt').textContent = 'Updated ' + state.index.generated_at;
  applyPreset('30');

  const preset = document.getElementById('rangePreset');
  const custom = document.getElementById('customRange');
  preset.addEventListener('change', async () => {
    if (preset.value === 'custom') { custom.hidden = false; return; }
    custom.hidden = true;
    applyPreset(preset.value);
    await redraw();
  });
  document.getElementById('applyRange').addEventListener('click', async () => {
    state.from = document.getElementById('fromDate').value;
    state.to   = document.getElementById('toDate').value;
    if (state.from && state.to) await redraw();
  });

  await redraw();
}

init().catch(err => {
  document.body.insertAdjacentHTML('afterbegin',
    `<pre style="color:#c00;padding:16px">Failed to load data: ${err.message}. If this is a brand-new repo, wait for the first daily run to finish.</pre>`);
});

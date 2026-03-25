const config = window.APP_CONFIG || {};

const messagesEl = document.getElementById("messages");
const formEl = document.getElementById("chat-form");
const inputEl = document.getElementById("query-input");
const sendButtonEl = document.getElementById("send-button");
const appMode = config.mode || "live";
let searchDataPromise = null;

function createMessage(role, innerHtml, badge = "") {
  const wrapper = document.createElement("article");
  wrapper.className = `message ${role}`;

  const meta = document.createElement("div");
  meta.className = "message-meta";
  meta.innerHTML = `
    <span class="message-role">${role === "assistant" ? "Assistant" : "You"}</span>
    ${badge ? `<span class="message-badge">${badge}</span>` : ""}
  `;

  const body = document.createElement("div");
  body.className = "message-body";
  body.innerHTML = innerHtml;

  wrapper.appendChild(meta);
  wrapper.appendChild(body);
  messagesEl.appendChild(wrapper);
  messagesEl.scrollTop = messagesEl.scrollHeight;
  return wrapper;
}

function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function assistantWelcome() {
  const intro = appMode === "static-search"
    ? `This GitHub Pages version runs retrieval directly in the browser using a prebuilt chunk index from the ICO coffee market reports.`
    : appMode === "static"
    ? `This is the static GitHub Pages preview of the chatbot interface. The visual layout, prompt ideas, and conversation shell are live here, but grounded retrieval still runs through the local Python server.`
    : `Ask grounded questions across ${config.reportCount || "multiple"} ICO coffee market reports. The assistant retrieves indexed report chunks, synthesizes a direct answer, and cites the supporting pages.`;

  createMessage(
    "assistant",
    `
      <section>
        <h3>What This Assistant Does</h3>
        <p>${intro}</p>
      </section>
      <section>
        <h3>Good Questions</h3>
        <p>${(config.suggestions || []).map((item) => escapeHtml(item)).join("<br />")}</p>
      </section>
    `,
    `${config.chunkCount || 0} indexed chunks`
  );
}

function renderSources(sources) {
  if (!sources || sources.length === 0) {
    return "";
  }

  return `
    <section>
      <h3>Sources</h3>
      <div class="source-list">
        ${sources
          .map(
            (source) => `
              <div class="source-card">
                <strong>${escapeHtml(source.title)}</strong>
                <span>Page ${source.page_number}</span>
              </div>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

function buildSeriesPolyline(points, width, height, paddingX, paddingY, minValue, maxValue) {
  const range = maxValue - minValue || 1;
  return points
    .map((point, index) => {
      const x = paddingX + (index * (width - paddingX * 2)) / Math.max(points.length - 1, 1);
      const y = height - paddingY - ((point.value - minValue) / range) * (height - paddingY * 2);
      return `${x},${y}`;
    })
    .join(" ");
}

function renderTrendChart(chart) {
  if (!chart || !chart.series || chart.series.length === 0) {
    return "";
  }

  const width = 640;
  const height = 220;
  const paddingX = 40;
  const paddingY = 24;
  const allPoints = chart.series.flatMap((series) => series.points);
  const values = allPoints.map((point) => point.value);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const xLabels = chart.series[0].points;

  const gridLines = Array.from({ length: 4 }, (_, index) => {
    const y = paddingY + (index * (height - paddingY * 2)) / 3;
    return `<line x1="${paddingX}" y1="${y}" x2="${width - paddingX}" y2="${y}" class="trend-grid-line" />`;
  }).join("");

  const polylines = chart.series
    .map((series) => {
      const points = buildSeriesPolyline(series.points, width, height, paddingX, paddingY, minValue, maxValue);
      const lastPoint = series.points[series.points.length - 1];
      return `
        <polyline points="${points}" fill="none" stroke="${series.color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
        <circle cx="${paddingX + ((series.points.length - 1) * (width - paddingX * 2)) / Math.max(series.points.length - 1, 1)}"
                cy="${height - paddingY - ((lastPoint.value - minValue) / (maxValue - minValue || 1)) * (height - paddingY * 2)}"
                r="4.5" fill="${series.color}"></circle>
      `;
    })
    .join("");

  const legend = chart.series
    .map(
      (series) => `
        <span class="trend-legend-item">
          <span class="trend-legend-dot" style="background:${series.color}"></span>
          ${escapeHtml(series.label)}
        </span>
      `
    )
    .join("");

  return `
    <section class="trend-card">
      <div class="trend-header">
        <div>
          <h3>Trend</h3>
          <p>${escapeHtml(chart.title)}</p>
          <span class="trend-subtitle">${escapeHtml(chart.subtitle || "")}</span>
        </div>
        <span class="trend-unit">${escapeHtml(chart.unit || "")}</span>
      </div>
      <svg viewBox="0 0 ${width} ${height}" class="trend-chart" role="img" aria-label="${escapeHtml(chart.title)}">
        ${gridLines}
        ${polylines}
        <text x="${paddingX}" y="${height - 4}" class="trend-axis-label">${escapeHtml(xLabels[0].label)}</text>
        <text x="${width - paddingX}" y="${height - 4}" text-anchor="end" class="trend-axis-label">${escapeHtml(xLabels[xLabels.length - 1].label)}</text>
      </svg>
      <div class="trend-legend">${legend}</div>
    </section>
  `;
}

function renderWhy(why) {
  if (!why || why.length === 0) {
    return "";
  }

  return `
    <section>
      <h3>Why</h3>
      <p>${escapeHtml(why.join(" "))}</p>
    </section>
  `;
}

function renderAssistantPayload(payload) {
  const answer = payload.answer
    ? escapeHtml(payload.answer)
    : "The current index did not return enough evidence to generate a concise answer.";

  return `
    <section>
      <h3>Answer</h3>
      <p>${answer}</p>
    </section>
    ${renderTrendChart(payload.trend_chart)}
    ${renderWhy(payload.why)}
    ${renderSources(payload.sources)}
  `;
}

function setLoadingState(isLoading) {
  sendButtonEl.disabled = isLoading;
  inputEl.disabled = isLoading;
}

function normalizeText(value) {
  return value.toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
}

function tokenize(value) {
  const stopWords = new Set(["what", "which", "where", "from", "with", "that", "have", "this", "recently", "about", "into", "than"]);
  return normalizeText(value)
    .split(" ")
    .filter((token) => token.length > 2 && !stopWords.has(token));
}

function splitSentences(text) {
  return text
    .replace(/\s*•\s*/g, ". ")
    .split(/(?<=[.!?])\s+/)
    .map((sentence) => sentence.trim())
    .filter(Boolean);
}

function normalizeSentence(text) {
  return text.replace(/\W+/g, "").toLowerCase();
}

function cleanCandidateSentence(sentence) {
  return sentence
    .replace(/Coffee Market Report\s*[–-]\s*[A-Za-z]+\s+\d{4}\s*\d*/g, "")
    .replace(/Figure\s+[A-Za-z0-9:.\- ]+/g, "")
    .replace(/^[-:;,.\s]+/, "")
    .replace(/\s+/g, " ")
    .trim();
}

function isUsableSentence(sentence) {
  const words = sentence.split(/\s+/);
  if (words.length < 10 || words.length > 55) {
    return false;
  }
  const lowered = sentence.toLowerCase();
  if (lowered.includes("figure") || lowered.includes("table") || lowered.includes("60-kg bags") || lowered.includes("60 -kg bags")) {
    return false;
  }
  const letters = [...sentence].filter((character) => /[a-z]/i.test(character)).length;
  const digits = [...sentence].filter((character) => /[0-9]/.test(character)).length;
  if (letters === 0 || digits > letters) {
    return false;
  }
  const numericTokens = words.filter((word) => /^[\d./%-]+$/.test(word)).length;
  return numericTokens <= 4 && !sentence.endsWith(":");
}

function sentenceScore(sentence, query, retrievalScore) {
  const querySet = new Set(tokenize(query));
  const sentenceSet = new Set(tokenize(sentence));
  let overlap = 0;
  querySet.forEach((term) => {
    if (sentenceSet.has(term)) {
      overlap += 1;
    }
  });
  return retrievalScore + overlap * 0.02;
}

function extractPriceDeclines(text) {
  const cleaned = text.replace(/\s+/g, " ");
  const sentences = cleaned.split(/(?<=[.!?])\s+/);
  const candidates = [];

  for (const sentence of sentences) {
    const pairedMatch = sentence.match(/Colombian Milds.? and Other Milds.? prices (?:retracted|decreased|declined) ([\d.]+)% and ([\d.]+)%/i);
    if (pairedMatch) {
      candidates.push({ label: "Colombian Milds", percentage: Number(pairedMatch[1]), sentence });
      candidates.push({ label: "Other Milds", percentage: Number(pairedMatch[2]), sentence });
    }

    const singlePatterns = [
      ["Brazilian Naturals", /Brazilian Naturals.? prices (?:shrank|decreased|declined|fell) ([\d.]+)%/gi],
      ["Robustas", /Robustas (?:declined|decreased|fell|contracted) by ([\d.]+)%/gi],
      ["Colombian Milds", /Colombian Milds.? prices (?:retracted|decreased|declined|fell) ([\d.]+)%/gi],
      ["Other Milds", /Other Milds.? prices (?:retracted|decreased|declined|fell) ([\d.]+)%/gi],
    ];

    for (const [label, pattern] of singlePatterns) {
      for (const match of sentence.matchAll(pattern)) {
        candidates.push({ label, percentage: Number(match[1]), sentence });
      }
    }
  }

  return candidates;
}

async function loadSearchData() {
  if (!searchDataPromise) {
    searchDataPromise = fetch(config.dataUrl || "./data/search-data.json")
      .then((response) => {
        if (!response.ok) {
          throw new Error("Failed to load static search data.");
        }
        return response.json();
      });
  }
  return searchDataPromise;
}

function scoreChunk(chunk, query, queryTokens, queryNormalized) {
  const title = normalizeText(chunk.title);
  const text = normalizeText(chunk.chunk_text);
  let score = 0;

  for (const token of queryTokens) {
    const titleMatches = title.split(token).length - 1;
    const textMatches = text.split(token).length - 1;
    score += titleMatches * 5;
    score += textMatches * 2;
  }

  if (queryNormalized && text.includes(queryNormalized)) {
    score += 8;
  }
  if (queryTokens.every((token) => text.includes(token) || title.includes(token))) {
    score += queryTokens.length * 2;
  }

  return score;
}

function searchStaticData(data, query, topK) {
  const queryTokens = tokenize(query);
  const queryNormalized = normalizeText(query);

  return data.chunks
    .map((chunk) => ({
      ...chunk,
      score: scoreChunk(chunk, query, queryTokens, queryNormalized),
    }))
    .filter((chunk) => chunk.score > 0)
    .sort((left, right) => right.score - left.score)
    .slice(0, topK);
}

function buildAnswer(results, query, maxSentences) {
  const queryLower = query.toLowerCase();

  if ((queryLower.includes("steepest") || queryLower.includes("worst")) &&
      ["price decline", "price performance", "performed worst", "performing worst"].some((phrase) => queryLower.includes(phrase))) {
    const declineCandidates = [];
    for (const result of results) {
      const source = `${result.title}, page ${result.page_number}`;
      for (const candidate of extractPriceDeclines(result.chunk_text)) {
        declineCandidates.push({ ...candidate, source });
      }
    }
    if (declineCandidates.length) {
      declineCandidates.sort((left, right) => right.percentage - left.percentage);
      const winner = declineCandidates[0];
      return {
        answer: `${winner.label} had the steepest price decline at ${winner.percentage.toFixed(1)}%.`,
        why: [winner.sentence],
        sourceKeys: [winner.source],
      };
    }
  }

  const rankedSentences = [];
  for (const result of results) {
    const source = `${result.title}, page ${result.page_number}`;
    for (const sentence of splitSentences(result.chunk_text)) {
      const cleaned = cleanCandidateSentence(sentence);
      if (!isUsableSentence(cleaned)) {
        continue;
      }
      rankedSentences.push({
        score: sentenceScore(cleaned, query, result.score / 10),
        sentence: cleaned,
        source,
      });
    }
  }

  rankedSentences.sort((left, right) => right.score - left.score);

  const selectedSentences = [];
  const selectedSources = [];
  const seenSentences = new Set();

  for (const item of rankedSentences) {
    const normalized = normalizeSentence(item.sentence);
    if (seenSentences.has(normalized)) {
      continue;
    }
    seenSentences.add(normalized);
    selectedSentences.push(item.sentence);
    if (!selectedSources.includes(item.source)) {
      selectedSources.push(item.source);
    }
    if (selectedSentences.length >= maxSentences) {
      break;
    }
  }

  return {
    answer: selectedSentences[0] || null,
    why: selectedSentences.slice(1),
    sourceKeys: selectedSources,
  };
}

function sourcesFromResults(results, sourceKeys) {
  const sourceMap = new Map(
    results.map((result) => [
      `${result.title}, page ${result.page_number}`,
      {
        title: result.title,
        page_number: result.page_number,
        report_id: result.report_id,
        published_date: result.published_date,
        source_url: result.source_url,
      },
    ])
  );

  return sourceKeys.filter((source) => sourceMap.has(source)).map((source) => sourceMap.get(source));
}

function inferTrendChart(trendData, query, answer) {
  if (!trendData || !trendData.series) {
    return null;
  }

  const series = trendData.series;
  const text = `${query} ${answer || ""}`.toLowerCase();

  if (["steepest price decline", "which coffee category", "performed worst", "performing worst", "compare"].some((phrase) => text.includes(phrase))) {
    const keys = ["colombian_milds", "other_milds", "brazilian_naturals", "robustas"];
    return {
      title: "ICO group price trend",
      subtitle: "Monthly average prices from the latest ICO report table",
      unit: "US cents/lb",
      series: keys.filter((key) => series[key]).map((key) => series[key]),
    };
  }

  const aliases = [
    ["robustas", ["robusta", "robustas"]],
    ["colombian_milds", ["colombian milds"]],
    ["other_milds", ["other milds"]],
    ["brazilian_naturals", ["brazilian naturals", "brazilian natural"]],
    ["new_york", ["new york"]],
    ["london", ["london"]],
  ];

  for (const [key, terms] of aliases) {
    if (terms.some((term) => text.includes(term)) && series[key]) {
      return {
        title: `${series[key].label} trend`,
        subtitle: "Monthly average prices from the latest ICO report table",
        unit: series[key].unit,
        series: [series[key]],
      };
    }
  }

  if (["price", "prices", "i-cip", "composite", "market performance", "decline"].some((keyword) => text.includes(keyword)) && series.ico_composite) {
    return {
      title: "ICO Composite Indicator Price trend",
      subtitle: "Monthly average prices from the latest ICO report table",
      unit: series.ico_composite.unit,
      series: [series.ico_composite],
    };
  }

  return null;
}

async function submitQuery(query) {
  createMessage("user", `<p>${escapeHtml(query)}</p>`);

  const loadingMessage = createMessage(
    "assistant",
    `<p><span class="loading-dot"></span><span class="loading-dot"></span><span class="loading-dot"></span> searching the coffee market reports</p>`
  );

  setLoadingState(true);

  try {
    loadingMessage.remove();

    if (appMode === "static-search") {
      const data = await loadSearchData();
      const results = searchStaticData(data, query, 5);
      const answerBundle = buildAnswer(results, query, 4);
      const payload = {
        answer: answerBundle.answer,
        why: answerBundle.why,
        sources: sourcesFromResults(results, answerBundle.sourceKeys),
        trend_chart: inferTrendChart(data.trend_data, query, answerBundle.answer),
      };
      createMessage("assistant", renderAssistantPayload(payload), "browser retrieval");
      return;
    }

    if (appMode === "static") {
      createMessage(
        "assistant",
        `
          <section>
            <h3>Static Preview</h3>
            <p>This GitHub Pages deployment is showing the chatbot UI only. To run live report retrieval and answer generation, start the local server with <code>${escapeHtml(config.localRunCommand || "python3 app/app.py --serve")}</code>.</p>
          </section>
        `,
        "static mode"
      );
      return;
    }

    const response = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });

    const payload = await response.json();
    if (!response.ok) {
      createMessage("assistant", `<p>${escapeHtml(payload.error || "Request failed.")}</p>`);
      return;
    }

    createMessage("assistant", renderAssistantPayload(payload), "grounded answer");
  } catch (error) {
    loadingMessage.remove();
    createMessage("assistant", `<p>Request failed. ${appMode === "static-search" ? "The static search index could not be loaded." : "Check that the local server is still running."}</p>`);
  } finally {
    setLoadingState(false);
    inputEl.focus();
  }
}

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  const query = inputEl.value.trim();
  if (!query) {
    return;
  }

  inputEl.value = "";
  await submitQuery(query);
});

document.querySelectorAll("[data-suggestion]").forEach((button) => {
  button.addEventListener("click", async () => {
    const query = button.getAttribute("data-suggestion");
    inputEl.value = "";
    await submitQuery(query);
  });
});

assistantWelcome();

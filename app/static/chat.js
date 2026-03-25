const config = window.APP_CONFIG || {};

const messagesEl = document.getElementById("messages");
const formEl = document.getElementById("chat-form");
const inputEl = document.getElementById("query-input");
const sendButtonEl = document.getElementById("send-button");
const appMode = config.mode || "live";

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
  const intro = appMode === "static"
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

async function submitQuery(query) {
  createMessage("user", `<p>${escapeHtml(query)}</p>`);

  if (appMode === "static") {
    createMessage(
      "assistant",
      `
        <section>
          <h3>Static Preview</h3>
          <p>This GitHub Pages deployment is showing the chatbot UI only. To run live report retrieval and answer generation, start the local server with <code>${escapeHtml(config.localRunCommand || "python3 app/app.py --serve")}</code>.</p>
        </section>
        <section>
          <h3>What You’ll Get Locally</h3>
          <p>Direct answer, short explanation, and cited ICO report pages from the indexed coffee market reports.</p>
        </section>
      `,
      "static mode"
    );
    return;
  }

  const loadingMessage = createMessage(
    "assistant",
    `<p><span class="loading-dot"></span><span class="loading-dot"></span><span class="loading-dot"></span> searching the coffee market reports</p>`
  );

  setLoadingState(true);

  try {
    const response = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });

    const payload = await response.json();
    loadingMessage.remove();

    if (!response.ok) {
      createMessage("assistant", `<p>${escapeHtml(payload.error || "Request failed.")}</p>`);
      return;
    }

    createMessage("assistant", renderAssistantPayload(payload), "grounded answer");
  } catch (error) {
    loadingMessage.remove();
    createMessage("assistant", `<p>Request failed. Check that the local server is still running.</p>`);
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

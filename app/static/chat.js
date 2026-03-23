const config = window.APP_CONFIG || {};

const messagesEl = document.getElementById("messages");
const formEl = document.getElementById("chat-form");
const inputEl = document.getElementById("query-input");
const sendButtonEl = document.getElementById("send-button");

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
  createMessage(
    "assistant",
    `
      <section>
        <h3>What This Assistant Does</h3>
        <p>Ask grounded questions across ${config.reportCount || "multiple"} ICO coffee market reports. The assistant retrieves indexed report chunks, synthesizes a direct answer, and cites the supporting pages.</p>
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

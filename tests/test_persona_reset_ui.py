from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PersonaResetUiTests(unittest.TestCase):
    def test_confirmation_and_request_lifecycle(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is not installed")
        app = (ROOT / "pages/companion-panel/app.js").read_text(encoding="utf-8")
        functions = app.split("function confirmPersonaReset(label) {", 1)[1].split(
            "\nfunction renderCurrentPersonaStatus(", 1
        )[0]
        locks = app.split("function setPersonaOperationBusy(busy) {", 1)[1].split(
            "\nfunction configSavedValue(", 1
        )[0]
        script = r'''
const assert = require("node:assert/strict");
const vm = require("node:vm");
const controls = Object.fromEntries(
  ["#resetCurrentPersonaBtn", "#pagePersonaSelect", "#configPersonaSelect"]
    .map(key => [key, { disabled: false }]),
);
const button = controls["#resetCurrentPersonaBtn"];
const dialogs = [];
const requests = [];
const toasts = [];
let failShow = false;
let failPost = false;
let finishPost = null;
let reloads = 0;
const context = {
  window: { confirm() { throw Error("native confirm is forbidden in the sandbox"); } },
  document: {
    body: { appendChild(dialog) { dialogs.push(dialog); } },
    createElement(tag) {
      assert.equal(tag, "dialog");
      const listeners = {};
      return {
        attributes: {}, returnValue: "", removed: false, open: false,
        setAttribute(key, value) { this.attributes[key] = value; },
        addEventListener(type, fn) { listeners[type] = fn; },
        showModal() { if (failShow) throw Error("cannot open dialog"); this.open = true; },
        close(value = "") { this.returnValue = value; this.open = false; listeners.close(); },
        remove() { this.removed = true; },
      };
    },
  },
  state: { personaOperationBusyCount: 0, lazyLoaded: {} },
  $: key => controls[key],
  pagePersonaRecords: () => [{ id: "one" }, { id: "two" }],
  escapeHtml: value => String(value).replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;").replaceAll("'", "&#39;"),
  postJson: (endpoint, payload) => {
    requests.push({ endpoint, payload });
    if (failPost) return Promise.reject(Error("reset failed"));
    return new Promise(resolve => { finishPost = resolve; });
  },
  showToast: (message, kind) => toasts.push({ message, kind }),
  setBookshelfUnlocked: () => {},
  resetBookshelfSelection: () => {},
  loadAll: async () => { reloads += 1; },
};
vm.createContext(context);
vm.runInContext(FUNCTIONS, context);
vm.runInContext(LOCKS, context);
const reset = context.resetPersonaFromPanel;
const flush = async () => { await Promise.resolve(); await Promise.resolve(); };
const assertUnlocked = () => {
  assert.equal(context.state.personaOperationBusyCount, 0);
  Object.values(controls).forEach(control => assert.equal(control.disabled, false));
};

(async () => {
  // Cancel and Escape both close a document dialog without a reset request.
  for (const value of ["cancel", ""]) {
    const pending = reset(button, "one", '<img src=x onerror="bad()">');
    assert.equal(requests.length, 0);
    Object.values(controls).forEach(control => assert.equal(control.disabled, true));
    const dialog = dialogs.at(-1);
    assert.equal(dialog.open, true);
    assert.equal(dialog.attributes["aria-labelledby"], "personaResetDialogTitle");
    assert.ok(dialog.innerHTML.includes("&lt;img"));
    assert.ok(!dialog.innerHTML.includes("<img"));
    assert.ok(dialog.innerHTML.includes('value="cancel" autofocus'));
    dialog.close(value);
    await pending;
    assert.equal(requests.length, 0);
    assert.equal(dialog.removed, true);
    assertUnlocked();
  }

  // Lock the target before confirmation and through the single in-flight POST.
  const pending = reset(button, "one", "Persona One");
  await reset(button, "two", "Persona Two");
  assert.equal(dialogs.length, 3);
  dialogs.at(-1).close("confirm");
  await flush();
  assert.equal(requests.length, 1);
  assert.equal(requests[0].endpoint, "/persona/reset-current");
  assert.equal(requests[0].payload.persona_id, "one");
  assert.equal(context.state.personaOperationBusyCount, 1);
  await reset(button, "two", "Persona Two");
  assert.equal(requests.length, 1);
  finishPost({ generation: 4 });
  await pending;
  assert.equal(reloads, 1);
  assertUnlocked();

  // Failed requests and failed dialog creation must release all locks.
  failPost = true;
  const failed = reset(button, "two", "Persona Two");
  dialogs.at(-1).close("confirm");
  await failed;
  assert.equal(requests.length, 2);
  assert.equal(toasts.at(-1).kind, "error");
  assert.equal(reloads, 1);
  assertUnlocked();
  failShow = true;
  await reset(button, "two", "Persona Two");
  assert.equal(dialogs.at(-1).removed, true);
  assert.equal(toasts.at(-1).kind, "error");
  assert.equal(requests.length, 2);
  assertUnlocked();
  console.log("cancel, Escape, target lock, duplicate guard, success and failure passed");
})().catch(error => { console.error(error); process.exitCode = 1; });
'''
        script = script.replace(
            "FUNCTIONS", json.dumps("function confirmPersonaReset(label) {" + functions)
        ).replace(
            "LOCKS", json.dumps("function setPersonaOperationBusy(busy) {" + locks)
        )
        completed = subprocess.run(
            [node, "-e", script], capture_output=True, text=True, timeout=15
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertIn("passed", completed.stdout)


if __name__ == "__main__":
    unittest.main()

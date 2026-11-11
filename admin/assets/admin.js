(function () {
  function get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }
  function set(k, v) { try { localStorage.setItem(k, v); } catch (e) {} }

  var alertBox = document.querySelector("[data-alert-key]");
  if (alertBox) {
    var key = alertBox.getAttribute("data-alert-key");
    if (get("dismissed-alert") === key) alertBox.hidden = true;
    var btn = document.getElementById("dismiss-alert");
    if (btn) btn.addEventListener("click", function () {
      set("dismissed-alert", key);
      alertBox.hidden = true;
    });
  }

  var toggle = document.getElementById("auto-refresh");
  if (toggle) {
    toggle.checked = get("auto-refresh") !== "off";
    toggle.addEventListener("change", function () { set("auto-refresh", toggle.checked ? "on" : "off"); });
  }

  var secs = parseInt(document.body.getAttribute("data-refresh"), 10) || 0;
  if (secs > 0) {
    setInterval(function () {
      if (toggle && !toggle.checked) return;
      var a = document.activeElement;
      if (a && /^(INPUT|SELECT|TEXTAREA|BUTTON)$/.test(a.tagName) && a !== toggle) return;
      if (document.querySelector("[data-busy]")) return;
      location.reload();
    }, secs * 1000);
  }
})();

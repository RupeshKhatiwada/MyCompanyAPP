(function () {
  "use strict";

  var MODE_AD = "AD";
  var MODE_BS = "BS";
  var DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

  var conversionCache = {
    ad_to_bs: new Map(),
    bs_to_ad: new Map()
  };

  function getCurrentMode() {
    var mode = (document.documentElement.dataset.calendarMode || MODE_AD).toUpperCase();
    return mode === MODE_BS ? MODE_BS : MODE_AD;
  }

  function isConversionAvailable() {
    return document.documentElement.dataset.calendarAvailable === "1";
  }

  async function convertMany(direction, rawValues) {
    var cache = conversionCache[direction];
    if (!cache) return {};

    var values = Array.from(new Set((rawValues || [])
      .map(function (value) { return String(value || "").trim(); })
      .filter(function (value) { return value.length > 0; })
    ));

    var result = {};
    var missing = [];

    values.forEach(function (value) {
      if (cache.has(value)) {
        result[value] = cache.get(value);
      } else {
        missing.push(value);
      }
    });

    if (!missing.length) return result;
    if (!isConversionAvailable()) {
      missing.forEach(function (value) {
        cache.set(value, null);
        result[value] = null;
      });
      return result;
    }

    var response = await fetch("/calendar/convert", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ direction: direction, values: missing })
    });

    if (!response.ok) {
      missing.forEach(function (value) {
        cache.set(value, null);
        result[value] = null;
      });
      return result;
    }

    var payload = null;
    try {
      payload = await response.json();
    } catch (err) {
      payload = null;
    }
    var converted = payload && payload.converted ? payload.converted : {};
    missing.forEach(function (value) {
      var convertedValue = Object.prototype.hasOwnProperty.call(converted, value)
        ? converted[value]
        : null;
      cache.set(value, convertedValue);
      result[value] = convertedValue;
    });

    return result;
  }

  function setupDateInput(input) {
    if (!input || input.dataset.bsBound === "1") return;
    input.dataset.bsBound = "1";

    var proxy = document.createElement("input");
    proxy.type = "text";
    proxy.className = "input bs-date-proxy";
    proxy.placeholder = "2082-01-01";
    proxy.autocomplete = "off";
    proxy.style.display = "none";

    var hint = document.createElement("p");
    hint.className = "mt-1 text-xs text-slate-500 bs-date-hint";
    hint.style.display = "none";

    async function syncProxyFromAd() {
      var adValue = String(input.value || "").trim();
      if (!adValue) {
        proxy.value = "";
        hint.textContent = "";
        return;
      }
      var mapped = await convertMany("ad_to_bs", [adValue]);
      var bsValue = mapped[adValue];
      proxy.value = bsValue || "";
      hint.textContent = bsValue ? ("AD: " + adValue) : "";
    }

    async function syncAdFromProxy() {
      var bsValue = String(proxy.value || "").trim();
      if (!bsValue) {
        input.value = "";
        proxy.setCustomValidity("");
        hint.textContent = "";
        return true;
      }
      if (!DATE_PATTERN.test(bsValue)) {
        proxy.setCustomValidity("Invalid BS date format (YYYY-MM-DD)");
        return false;
      }
      var mapped = await convertMany("bs_to_ad", [bsValue]);
      var adValue = mapped[bsValue];
      if (!adValue) {
        proxy.setCustomValidity("Invalid BS date");
        return false;
      }
      input.value = adValue;
      proxy.setCustomValidity("");
      hint.textContent = "AD: " + adValue;
      return true;
    }

    proxy.addEventListener("change", function () {
      void syncAdFromProxy();
    });
    proxy.addEventListener("blur", function () {
      void syncAdFromProxy();
    });
    input.addEventListener("change", function () {
      void syncProxyFromAd();
    });

    input.insertAdjacentElement("afterend", proxy);
    proxy.insertAdjacentElement("afterend", hint);

    input._bsProxy = proxy;
    input._bsHint = hint;
    input._syncBsToAd = syncAdFromProxy;
    input._syncAdToBs = syncProxyFromAd;

    void syncProxyFromAd();
  }

  async function refreshDateInputMode() {
    var mode = getCurrentMode();
    var available = isConversionAvailable();
    var inputs = document.querySelectorAll('input[type="date"]');
    for (var i = 0; i < inputs.length; i += 1) {
      var input = inputs[i];
      setupDateInput(input);
      var proxy = input._bsProxy;
      var hint = input._bsHint;
      if (!proxy || !hint) continue;

      if (!available) {
        input.style.display = "";
        proxy.style.display = "none";
        hint.style.display = "none";
        proxy.setCustomValidity("");
        continue;
      }

      if (mode === MODE_BS) {
        input.style.display = "none";
        proxy.style.display = "block";
        hint.style.display = "block";
        await input._syncAdToBs();
      } else {
        input.style.display = "";
        proxy.style.display = "none";
        hint.style.display = "none";
        proxy.setCustomValidity("");
      }
    }
  }

  function getDateTextNodes() {
    var nodes = document.querySelectorAll("td,th,p,span,div,a,strong,label");
    var list = [];
    for (var i = 0; i < nodes.length; i += 1) {
      var node = nodes[i];
      if (!node || node.children.length > 0) continue;
      var text = String(node.textContent || "").trim();
      if (DATE_PATTERN.test(text)) {
        if (!node.dataset.calendarAdDate) {
          node.dataset.calendarAdDate = text;
        }
        list.push(node);
      } else if (node.dataset.calendarAdDate) {
        list.push(node);
      }
    }
    return list;
  }

  async function refreshDateTextMode() {
    var mode = getCurrentMode();
    var available = isConversionAvailable();
    var nodes = getDateTextNodes();
    if (!nodes.length) return;

    if (mode === MODE_AD || !available) {
      nodes.forEach(function (node) {
        var ad = node.dataset.calendarAdDate;
        if (ad) {
          node.textContent = ad;
          node.title = "";
        }
      });
      return;
    }

    var adValues = Array.from(new Set(nodes
      .map(function (node) { return node.dataset.calendarAdDate; })
      .filter(function (value) { return DATE_PATTERN.test(String(value || "")); })
    ));

    var mapped = await convertMany("ad_to_bs", adValues);
    nodes.forEach(function (node) {
      var ad = node.dataset.calendarAdDate;
      if (!ad) return;
      var bs = mapped[ad];
      if (bs) {
        node.textContent = bs;
        node.title = "AD: " + ad;
      } else {
        node.textContent = ad;
        node.title = "";
      }
    });
  }

  function bindFormSubmitSync() {
    var forms = document.querySelectorAll("form");
    forms.forEach(function (form) {
      if (form.dataset.bsSubmitBound === "1") return;
      form.dataset.bsSubmitBound = "1";
      form.addEventListener("submit", function (event) {
        if (getCurrentMode() !== MODE_BS) return;
        if (!isConversionAvailable()) return;
        if (form.dataset.bsSubmitting === "1") return;

        var visibleProxies = Array.from(form.querySelectorAll(".bs-date-proxy"))
          .filter(function (proxy) {
            return proxy.offsetParent !== null;
          });
        if (!visibleProxies.length) return;

        event.preventDefault();
        form.dataset.bsSubmitting = "1";

        (async function () {
          var valid = true;
          for (var i = 0; i < visibleProxies.length; i += 1) {
            var proxy = visibleProxies[i];
            var dateInput = proxy.previousElementSibling;
            if (!dateInput || typeof dateInput._syncBsToAd !== "function") continue;
            var ok = await dateInput._syncBsToAd();
            if (!ok) {
              valid = false;
              proxy.reportValidity();
              break;
            }
          }
          form.dataset.bsSubmitting = "0";
          if (valid) {
            form.submit();
          }
        })().catch(function () {
          form.dataset.bsSubmitting = "0";
        });
      });
    });
  }

  async function refreshCalendarUi() {
    await refreshDateInputMode();
    await refreshDateTextMode();
    bindFormSubmitSync();
  }

  function init() {
    void refreshCalendarUi();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

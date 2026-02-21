const express = require("express");
const dayjs = require("dayjs");
const { db } = require("../db");
const { requireRole } = require("../middleware/auth");

const router = express.Router();

const AI_PROVIDER = process.env.AI_PROVIDER || "auto";
const AI_BASE_URL = process.env.AI_BASE_URL || "http://localhost:11434";
const AI_MODEL = process.env.AI_MODEL || "llama3.1:8b";
const AI_ENABLED = process.env.AI_ENABLED !== "0";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1";

const detectLang = (text) => (/[\u0900-\u097F]/.test(text) ? "ne" : "en");

const buildPeriod = (message) => {
  const lower = message.toLowerCase();
  if (lower.includes("today") || lower.includes("आज")) {
    const date = dayjs().format("YYYY-MM-DD");
    return { from: date, to: date };
  }
  if (lower.includes("this month") || lower.includes("यो महिना")) {
    return { from: dayjs().startOf("month").format("YYYY-MM-DD"), to: dayjs().format("YYYY-MM-DD") };
  }
  if (lower.includes("this year") || lower.includes("यो वर्ष")) {
    return { from: dayjs().startOf("year").format("YYYY-MM-DD"), to: dayjs().format("YYYY-MM-DD") };
  }
  if (lower.includes("this week") || lower.includes("यो हप्ता") || lower.includes("this weak")) {
    return { from: dayjs().subtract(6, "day").format("YYYY-MM-DD"), to: dayjs().format("YYYY-MM-DD") };
  }
  return { from: dayjs().subtract(6, "day").format("YYYY-MM-DD"), to: dayjs().format("YYYY-MM-DD") };
};

const getQuickAnswer = (req, message) => {
  const lower = message.toLowerCase();
  const { from, to } = buildPeriod(message);

  if (lower.includes("most credit") || lower.includes("सबैभन्दा बढी उधार") || lower.includes("most outstanding")) {
    const topCustomer = db.prepare(
      `SELECT customer_name, SUM(amount - paid_amount) as remaining
       FROM credits
       WHERE credit_date BETWEEN ? AND ?
       GROUP BY customer_name
       ORDER BY remaining DESC
       LIMIT 1`
    ).get(from, to);

    const topVehicle = db.prepare(
      `SELECT vehicles.vehicle_number, SUM(credit_amount - paid_amount) as remaining
       FROM exports
       JOIN vehicles ON exports.vehicle_id = vehicles.id
       WHERE export_date BETWEEN ? AND ?
       GROUP BY exports.vehicle_id
       ORDER BY remaining DESC
       LIMIT 1`
    ).get(from, to);

    const customerText = topCustomer && topCustomer.customer_name
      ? `${topCustomer.customer_name} (${Math.max(0, Number(topCustomer.remaining || 0)).toFixed(2)})`
      : req.t("noData");
    const vehicleText = topVehicle && topVehicle.vehicle_number
      ? `${topVehicle.vehicle_number} (${Math.max(0, Number(topVehicle.remaining || 0)).toFixed(2)})`
      : req.t("noData");

    return req.t("aiMostCreditAnswer", { from, to, customer: customerText, vehicle: vehicleText });
  }

  if (lower.includes("total export") || lower.includes("total sales") || lower.includes("कुल बिक्री") || lower.includes("कुल ढुवानी")) {
    const totals = db.prepare(
      `SELECT COALESCE(SUM(total_amount), 0) as total
       FROM exports
       WHERE export_date BETWEEN ? AND ?`
    ).get(from, to);
    return req.t("aiTotalExportAnswer", { from, to, total: Number(totals.total || 0).toFixed(2) });
  }

  if (lower.includes("leakage") || lower.includes("लिकेज")) {
    const totals = db.prepare(
      `SELECT COALESCE(SUM(leakage_jar_count), 0) as leakage
       FROM exports
       WHERE export_date BETWEEN ? AND ?`
    ).get(from, to);
    return req.t("aiLeakageAnswer", { from, to, total: Number(totals.leakage || 0) });
  }

  if (lower.includes("returns") || lower.includes("फिर्ता")) {
    const totals = db.prepare(
      `SELECT COALESCE(SUM(return_jar_count), 0) as return_jars
       FROM exports
       WHERE export_date BETWEEN ? AND ?`
    ).get(from, to);
    return req.t("aiReturnAnswer", { from, to, total: Number(totals.return_jars || 0) });
  }

  if (lower.includes("jar sales") || lower.includes("empty jar") || lower.includes("जार बिक्री")) {
    const totals = db.prepare(
      `SELECT COALESCE(SUM(total_amount), 0) as total
       FROM jar_sales
       WHERE sale_date BETWEEN ? AND ?`
    ).get(from, to);
    return req.t("aiJarSalesAnswer", { from, to, total: Number(totals.total || 0).toFixed(2) });
  }

  if (lower.includes("trip") || lower.includes("ट्रिप")) {
    const totals = db.prepare(
      `SELECT COUNT(*) as trips
       FROM exports
       WHERE export_date BETWEEN ? AND ?`
    ).get(from, to);
    return req.t("aiTripAnswer", { from, to, total: Number(totals.trips || 0) });
  }

  if (lower.includes("find") || lower.includes("खोज") || lower.includes("search")) {
    const term = message.replace(/find|search|खोज/gi, "").trim();
    if (!term) return null;
    const like = `%${term}%`;
    const vehicles = db.prepare(
      "SELECT vehicle_number FROM vehicles WHERE vehicle_number LIKE ? OR owner_name LIKE ? LIMIT 5"
    ).all(like, like);
    const customers = db.prepare(
      "SELECT DISTINCT customer_name FROM credits WHERE customer_name LIKE ? LIMIT 5"
    ).all(like).map((row) => row.customer_name);
    const jarTypes = db.prepare(
      "SELECT name FROM jar_types WHERE name LIKE ? LIMIT 5"
    ).all(like).map((row) => row.name);
    return req.t("aiSearchAnswer", {
      term,
      vehicles: vehicles.map((v) => v.vehicle_number).join(", ") || "-",
      customers: customers.join(", ") || "-",
      jarTypes: jarTypes.join(", ") || "-"
    });
  }

  return null;
};

const buildContext = () => {
  const today = dayjs().format("YYYY-MM-DD");
  const weekStart = dayjs().subtract(6, "day").format("YYYY-MM-DD");
  const monthStart = dayjs().startOf("month").format("YYYY-MM-DD");

  const todayExport = db.prepare(
    "SELECT COALESCE(SUM(total_amount), 0) as total, COALESCE(SUM(credit_amount - paid_amount), 0) as credit FROM exports WHERE export_date = ?"
  ).get(today);
  const weekExport = db.prepare(
    "SELECT COALESCE(SUM(total_amount), 0) as total FROM exports WHERE export_date BETWEEN ? AND ?"
  ).get(weekStart, today);
  const monthExport = db.prepare(
    "SELECT COALESCE(SUM(total_amount), 0) as total FROM exports WHERE export_date BETWEEN ? AND ?"
  ).get(monthStart, today);
  const outstandingCredits = db.prepare(
    "SELECT COALESCE(SUM(amount - paid_amount), 0) as remaining FROM credits"
  ).get();

  return {
    today,
    weekStart,
    monthStart,
    todayExport,
    weekExport,
    monthExport,
    outstandingCredits
  };
};

const extractOutputText = (data) => {
  if (!data) return null;
  if (typeof data.output_text === "string" && data.output_text.trim()) {
    return data.output_text.trim();
  }
  const output = Array.isArray(data.output) ? data.output : [];
  const parts = [];
  for (const item of output) {
    if (item?.type === "message" && Array.isArray(item.content)) {
      for (const part of item.content) {
        if (part?.type === "output_text" && part.text) parts.push(part.text);
      }
    }
  }
  const text = parts.join("\n").trim();
  return text || null;
};

const callOpenAI = async ({ message, systemPrompt }) => {
  if (!AI_ENABLED || !OPENAI_API_KEY) return null;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(`${OPENAI_BASE_URL}/responses`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: OPENAI_MODEL,
        input: message,
        instructions: systemPrompt,
        temperature: 0.2
      }),
      signal: controller.signal
    });
    clearTimeout(timeout);
    if (!res.ok) return null;
    const data = await res.json();
    return extractOutputText(data);
  } catch (err) {
    return null;
  }
};

const callAI = async ({ message, systemPrompt }) => {
  if (!AI_ENABLED) return null;
  try {
    if (AI_PROVIDER === "openai") {
      return await callOpenAI({ message, systemPrompt });
    }
    if (AI_PROVIDER === "ollama") {
      const res = await fetch(`${AI_BASE_URL}/api/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: AI_MODEL,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: message }
          ],
          stream: false,
          options: { temperature: 0.2 }
        })
      });
      if (!res.ok) return null;
      const data = await res.json();
      return data?.message?.content || null;
    }
    if (AI_PROVIDER === "auto") {
      const openaiReply = await callOpenAI({ message, systemPrompt });
      if (openaiReply) return openaiReply;
      const res = await fetch(`${AI_BASE_URL}/api/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: AI_MODEL,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: message }
          ],
          stream: false,
          options: { temperature: 0.2 }
        })
      });
      if (!res.ok) return null;
      const data = await res.json();
      return data?.message?.content || null;
    }

    const res = await fetch(`${AI_BASE_URL}/v1/chat/completions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: AI_MODEL,
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: message }
        ],
        temperature: 0.2
      })
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data?.choices?.[0]?.message?.content || null;
  } catch (err) {
    return null;
  }
};

router.use(requireRole(["SUPER_ADMIN", "ADMIN", "WORKER"]));

router.get("/", (req, res) => {
  const openAIEnabled = AI_ENABLED && OPENAI_API_KEY && (AI_PROVIDER === "openai" || AI_PROVIDER === "auto");
  res.render("assistant", { title: req.t("assistantTitle"), openAIEnabled });
});

router.post("/ask", async (req, res) => {
  const message = String(req.body?.message || "").trim();
  if (!message) {
    return res.json({ answer: req.t("assistantEmpty") });
  }

  const quick = getQuickAnswer(req, message);
  if (quick) return res.json({ answer: quick, source: "rules" });

  const lang = detectLang(message);
  const context = buildContext();
  const contextText = [
    `Today: ${context.today}`,
    `This week: ${context.weekStart} to ${context.today}`,
    `This month: ${context.monthStart} to ${context.today}`,
    `Today exports total: ${Number(context.todayExport.total || 0).toFixed(2)}`,
    `Today outstanding export credit: ${Number(context.todayExport.credit || 0).toFixed(2)}`,
    `Week exports total: ${Number(context.weekExport.total || 0).toFixed(2)}`,
    `Month exports total: ${Number(context.monthExport.total || 0).toFixed(2)}`,
    `Outstanding customer credits: ${Number(context.outstandingCredits.remaining || 0).toFixed(2)}`
  ].join("\n");

  const basePrompt = lang === "ne"
    ? "तपाईं AQUA MSK सहायक हुनुहुन्छ। सानो, स्पष्ट उत्तर दिनुहोस्।"
    : "You are AQUA MSK assistant. Reply briefly and clearly.";
  const systemPrompt = `${basePrompt}\n\nContext:\n${contextText}`;

  if (AI_PROVIDER === "openai" && !OPENAI_API_KEY) {
    return res.json({ answer: req.t("assistantKeyMissing") });
  }

  const aiReply = await callAI({ message, systemPrompt });

  if (!aiReply) {
    return res.json({ answer: req.t("assistantError"), source: "fallback" });
  }
  return res.json({ answer: aiReply, source: "ai" });
});

module.exports = router;

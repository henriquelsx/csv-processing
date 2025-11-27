// csv-worker/src/worker.js
require("dotenv").config();

const fs = require("fs");
const csv = require("csv-parser");
const amqp = require("amqplib");
const { Pool } = require("pg");
const path = require("path");

// Postgres pool (usa DATABASE_URL no .env do worker)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// DB helpers (parametrizados)
async function updateJob(jobId, fields) {
  const keys = Object.keys(fields);
  if (keys.length === 0) return;

  const sets = keys.map((k, i) => `${k} = $${i + 1}`).join(", ");
  const values = keys.map(k => fields[k]);

  const query = `UPDATE jobs SET ${sets} WHERE id = $${keys.length + 1}`;
  values.push(jobId);

  await pool.query(query, values);
}

async function insertJobError(jobId, lineNumber, errorMessage) {
  try {
    await pool.query(
      `INSERT INTO job_errors (job_id, line_number, error_message)
       VALUES ($1, $2, $3)`,
      [jobId, lineNumber, errorMessage]
    );
  } catch (err) {
    // não falhar o job só por não conseguir gravar o erro
    console.error("[DB] Falha ao salvar job_error:", err.message);
  }
}

async function finishJob(jobId, total, processed, errors) {
  await updateJob(jobId, {
    total_rows: total,
    processed_rows: processed,
    error_rows: errors,
    status: errors > 0 ? "DONE_WITH_ERRORS" : "DONE",
    finished_at: new Date(),
  });
}

// conta linhas de forma rápida (conta bytes \n)
async function countLines(filepath) {
  return new Promise((resolve, reject) => {
    let count = 0;
    const stream = fs.createReadStream(filepath);
    stream.on("data", (buf) => {
      for (let i = 0; i < buf.length; i++) {
        if (buf[i] === 10) count++; // \n
      }
    });
    stream.on("end", () => resolve(count));
    stream.on("error", reject);
  });
}

// PROCESSAMENTO DO CSV
async function processCsv(jobId, filepath, channel, msg) {
  console.log("[STEP] Validando arquivo:", filepath);

  if (!fs.existsSync(filepath)) {
    console.log("[ERROR] Arquivo não existe");
    await updateJob(jobId, { status: "ERROR" });
    channel.ack(msg);
    return;
  }

  // contagem prévia
  const total = await countLines(filepath);
  console.log(`[OK] Total de linhas: ${total}`);
  await updateJob(jobId, { total_rows: total });

  let processed = 0;
  let errors = 0;
  let lineNumber = 0;
  const startedAt = Date.now();
  let lastProgressAt = Date.now();
  let finished = false;

  // função guard para finalizar somente uma vez
  async function finalize() {
    if (finished) return;
    finished = true;
    const elapsedSec = (Date.now() - startedAt) / 1000;
    const speed = elapsedSec > 0 ? (processed / elapsedSec).toFixed(2) : "0.00";
    console.log(`\n[FINISH] Processamento finalizado — processed=${processed} errors=${errors} total=${total} (${speed} l/s)`);
    await finishJob(jobId, total, processed, errors);
    channel.ack(msg);
  }

  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filepath)
      .pipe(csv());

    stream.on("data", async (row) => {
      // incrementa linha
      lineNumber++;
      try {
        // ===== AQUI INSERE O PROCESSAMENTO REAL =====
        // Ex: validar e inserir no banco, chamar outro serviço, etc.
        // await processRow(row); // se usar await, cuidado com concorrência (ver abaixo)
        // ===========================================

        processed++;
      } catch (err) {
        errors++;
        await insertJobError(jobId, lineNumber, err.message || String(err));
      }

      // atualiza progresso periodicamente (a cada 1s)
      const now = Date.now();
      if (now - lastProgressAt >= 1000) {
        lastProgressAt = now;
        const elapsedSec = (now - startedAt) / 1000;
        const speed = elapsedSec > 0 ? (processed / elapsedSec) : 0;
        const percent = total > 0 ? ((processed / total) * 100).toFixed(1) : "0.0";
        const eta = speed > 0 ? Math.round((total - processed) / speed) : null;

        console.log(`[PROGRESS] ${percent}% | ${processed}/${total} | ${speed.toFixed(2)} l/s | ETA ${eta !== null ? eta + "s" : "-"}`);

        // atualiza DB (batched, 1s)
        try {
          await updateJob(jobId, {
            processed_rows: processed,
            error_rows: errors,
            status: "PROCESSING",
          });
        } catch (err) {
          console.error("[DB] erro ao atualizar progresso:", err.message);
        }
      }
    });

    stream.once("end", async () => {
      try {
        await finalize();
        resolve();
      } catch (err) {
        reject(err);
      }
    });

    stream.once("error", async (err) => {
      console.error("[STREAM] erro:", err.message || err);
      try {
        await updateJob(jobId, { status: "ERROR" });
      } catch (e) {
        console.error("[DB] erro ao marcar ERROR:", e.message);
      }
      // ack para não requeue infinito; se quiser requeue, usar channel.nack(msg, false, true)
      channel.ack(msg);
      reject(err);
    });
  });
}

// START WORKER (connecta RabbitMQ e consome)
async function startWorker() {
  console.log("=== Worker iniciando ===");
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertQueue(process.env.RABBITMQ_QUEUE, { durable: true });
  channel.prefetch(1);
  console.log("[WORKER] Aguardando jobs na fila:", process.env.RABBITMQ_QUEUE);

  channel.consume(process.env.RABBITMQ_QUEUE, async (msg) => {
    if (!msg) return;
    let data;
    try {
      data = JSON.parse(msg.content.toString());
    } catch (err) {
      console.error("[WORKER] mensagem inválida:", err.message);
      channel.ack(msg);
      return;
    }

    const { jobId, filepath } = data;
    console.log(`\n[EVENT] Job recebido: ${jobId}`);
    try {
      await updateJob(jobId, { status: "PROCESSING" });
      await processCsv(jobId, filepath, channel, msg);
    } catch (err) {
      console.error("[WORKER] erro processing job:", err.message || err);
      try {
        await updateJob(jobId, { status: "ERROR" });
      } catch (e) {
        console.error("[DB] erro ao atualizar status ERROR:", e.message);
      }
      channel.ack(msg);
    }
  }, { noAck: false });
}

startWorker().catch((err) => {
  console.error("[FATAL] worker falhou:", err);
  process.exit(1);
});

  

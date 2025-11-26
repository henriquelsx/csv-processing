// src/worker.js — worker com logs detalhados para debug
require("dotenv").config();
const fs = require("fs");
const csv = require("csv-parser");
const amqp = require("amqplib");
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

(async () => {
  console.log("=== WORKER DEBUG START ===");
  try {
    console.log("[STEP] Verificando variáveis de ambiente...");
    console.log(" RABBITMQ_URL:", process.env.RABBITMQ_URL);
    console.log(" RABBITMQ_QUEUE:", process.env.RABBITMQ_QUEUE);
    console.log(" DATABASE_URL:", process.env.DATABASE_URL ? "[OK]" : "[MISSING]");

    console.log("[STEP] Tentando conectar no RabbitMQ...");
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    console.log("[OK] Conectado ao RabbitMQ");

    console.log("[STEP] Criando canal...");
    const channel = await connection.createChannel();
    console.log("[OK] Canal criado");

    console.log(`[STEP] Declarando/assegurando fila: ${process.env.RABBITMQ_QUEUE}`);
    await channel.assertQueue(process.env.RABBITMQ_QUEUE, { durable: true });
    console.log("[OK] Fila assegurada");

    console.log("Worker aguardando mensagens (CTRL+C para sair)...");
    channel.consume(process.env.RABBITMQ_QUEUE, async (msg) => {
      console.log("----\n[EVENT] Mensagem recebida do RabbitMQ");
      if (!msg) {
        console.log("[WARN] msg === null, retornando.");
        return;
      }

      let payload;
      try {
        const text = msg.content.toString();
        console.log("[STEP] Raw message content:", text);
        payload = JSON.parse(text);
        console.log("[OK] Mensagem parseada:", payload);
      } catch (err) {
        console.error("[ERROR] Falha ao parsear JSON da mensagem:", err);
        // ack para não travar a fila com mensagem malformada
        channel.ack(msg);
        return;
      }

      const { jobId, filepath } = payload;
      if (!jobId) {
        console.error("[ERROR] jobId inexistente na mensagem:", payload);
        channel.ack(msg);
        return;
      }
      if (!filepath) {
        console.error("[ERROR] filepath inexistente na mensagem:", payload);
        channel.ack(msg);
        return;
      }

      console.log(`[STEP] Verificando existência do arquivo: ${filepath}`);
      if (!fs.existsSync(filepath)) {
        console.error("[ERROR] Arquivo NÃO existe no caminho informado:", filepath);
        // marcar job como FAILED no DB (opcional)
        try {
          await pool.query(
            `UPDATE jobs SET status = 'FAILED', updated_at = NOW() WHERE id = $1`,
            [jobId]
          );
          console.log("[DB] Job marcado como FAILED (arquivo não encontrado).");
        } catch (dbErr) {
          console.error("[DB ERROR] ao setar FAILED:", dbErr);
        }
        channel.ack(msg);
        return;
      }
      console.log("[OK] Arquivo encontrado.");

      // Atualiza status no DB para PROCESSING
      try {
        console.log("[STEP] Atualizando job status -> PROCESSING");
        await pool.query(
          `UPDATE jobs SET status = 'PROCESSING', updated_at = NOW() WHERE id = $1`,
          [jobId]
        );
        console.log("[DB] Job atualizado para PROCESSING");
      } catch (dbErr) {
        console.error("[DB ERROR] ao atualizar status para PROCESSING:", dbErr);
        // ack pra evitar loop; considere nack se quiser reprocessar
        channel.ack(msg);
        return;
      }

      // Processamento em streaming
      console.log("[STEP] Iniciando leitura do CSV em streaming...");
      let processed = 0;
      let errors = 0;
      let total = 0;

      const stream = fs.createReadStream(filepath).pipe(csv());

      // proteger listener com try/catch dentro do on('data')
      stream.on("data", async (row) => {
        // NOTA: callbacks estão em paralelo — cuidado com muitas queries síncronas
        total++;
        try {
          // Simulação de validação/processamento
          // Substitua pela sua lógica real aqui
          if (!row || Object.keys(row).length === 0) {
            throw new Error("linha vazia");
          }
          processed++;
        } catch (procErr) {
          errors++;
          console.error(`[ROW ERROR] row #${total} -> ${procErr.message}`);
          try {
            await pool.query(
              `INSERT INTO job_errors (job_id, row_number, row_data, error_message) VALUES ($1, $2, $3, $4)`,
              [jobId, total, JSON.stringify(row), procErr.message]
            );
            console.log(`[DB] job_errors inserido para row #${total}`);
          } catch (dbErrRow) {
            console.error("[DB ERROR] ao inserir job_errors:", dbErrRow);
          }
        }

        // Atualiza contadores no DB a cada N linhas (ou sempre, se preferir)
        if (total % 10 === 0) {
          try {
            await pool.query(
              `UPDATE jobs SET processed_rows = $1, error_rows = $2, total_rows = $3, updated_at = NOW() WHERE id = $4`,
              [processed, errors, total, jobId]
            );
            console.log(`[DB] Progresso salvo: processed=${processed} errors=${errors} total=${total}`);
          } catch (dbErrProgress) {
            console.error("[DB ERROR] ao salvar progresso:", dbErrProgress);
          }
        }
      });

      stream.on("end", async () => {
        console.log("[STEP] Stream terminou. total, processed, errors =", total, processed, errors);
        try {
          await pool.query(
            `UPDATE jobs SET processed_rows = $1, error_rows = $2, total_rows = $3, status = 'COMPLETED', updated_at = NOW() WHERE id = $4`,
            [processed, errors, total, jobId]
          );
          console.log("[DB] Job atualizado para COMPLETED");
        } catch (dbErrEnd) {
          console.error("[DB ERROR] ao finalizar job:", dbErrEnd);
        }
        channel.ack(msg);
        console.log("----\n[OK] Mensagem ACKed e job finalizado.");
      });

      stream.on("error", async (err) => {
        console.error("[STREAM ERROR] Erro lendo o CSV:", err);
        try {
          await pool.query(
            `UPDATE jobs SET status = 'FAILED', updated_at = NOW() WHERE id = $1`,
            [jobId]
          );
          console.log("[DB] Job marcado como FAILED devido a erro no stream");
        } catch (dbErrStream) {
          console.error("[DB ERROR] ao marcar FAILED:", dbErrStream);
        }
        channel.ack(msg);
      });

    });
  } catch (err) {
    console.error("[FATAL] Erro no worker (startup):", err);
    process.exit(1);
  }
})();

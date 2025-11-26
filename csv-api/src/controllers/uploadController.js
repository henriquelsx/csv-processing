const multer = require("multer");
const path = require("path");
const db = require("../services/db");
const connectRabbit = require("../services/rabbit");

const upload = multer({
  dest: path.join(__dirname, "../../uploads")
});

exports.uploadCSV = async (req, res) => {
  upload.single("file")(req, res, async (err) => {
    if (err) return res.status(500).json({ error: "Erro no upload" });

    const filepath = req.file.path;

    // cria job no postgres
    const result = await db.query(
      "INSERT INTO jobs (filename, filepath, status) VALUES ($1, $2, $3) RETURNING id",
      [req.file.originalname, filepath, "PENDING"]
    );

    const jobId = result.rows[0].id;

    // envia para a fila
    const channel = await connectRabbit();
    const message = JSON.stringify({ jobId, filepath });

    channel.sendToQueue(process.env.RABBITMQ_QUEUE, Buffer.from(message), {
      persistent: true,
    });

    return res.json({
      jobId,
      status: "PENDING",
      message: "Arquivo recebido e enviado para processamento."
    });
  });
};

exports.getJobStatus = async (req, res) => {
  const { id } = req.params;

  const result = await db.query("SELECT * FROM jobs WHERE id = $1", [id]);

  if (result.rows.length === 0) {
    return res.status(404).json({ error: "Job não encontrado" });
  }

  return res.json(result.rows[0]);
};

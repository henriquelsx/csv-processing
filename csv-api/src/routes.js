const express = require("express");
const router = express.Router();
const uploadController = require("./controllers/uploadController");

router.post("/upload", uploadController.uploadCSV);
router.get("/jobs/:id", uploadController.getJobStatus);

module.exports = router;

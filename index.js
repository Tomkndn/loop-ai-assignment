const express = require("express");
const { v4: uuidv4 } = require("uuid");

const app = express();
app.use(express.json());
const PORT = 5000;

// In-memory DB for ingestion status
const ingestionStore = {};
// Global queue
const batchQueue = []; 

const PRIORITY_MAP = {
  HIGH: 1,
  MEDIUM: 2,
  LOW: 3,
};

// Ingestion APIS

// adding the ingest in the queue
app.post("/ingest", (req, res) => {
  const { ids, priority } = req.body;

// the ids and priority should be valid
  if (
    !Array.isArray(ids) ||
    ids.some((id) => typeof id !== "number") ||
    !PRIORITY_MAP[priority]
  ) {
    return res
      .status(400)
      .json({
        error: "Invalid payload: Provide an array of IDs and a valid priority",
      });
  }

  const ingestion_id = uuidv4();
  const created_time = Date.now();
  const batches = [];

// splitting the ids into batches
  for (let i = 0; i < ids.length; i += 3) {
    const batch_ids = ids.slice(i, i + 3);
    const batch_id = uuidv4();
    const batch = {
      batch_id,
      ids: batch_ids,
      status: "yet_to_start",
      ingestion_id,
      priority,
      created_time,
    };
    batches.push(batch);
    batchQueue.push(batch);
  }

  ingestionStore[ingestion_id] = {
    ingestion_id,
    status: "yet_to_start",
    batches,
  };

  res.status(202).json({ ingestion_id });
});


// getting the ingestion status
app.get("/status/:id", (req, res) => {
    const ingestion = ingestionStore[req.params.id];
    
    // if the ingestion id is not found
  if (!ingestion)
    return res.status(404).json({ error: "Ingestion ID not found" });

  const statuses = ingestion.batches.map((b) => b.status);

  let overallStatus = "yet_to_start";
  if (statuses.every((s) => s === "completed")) {
    overallStatus = "completed";
  } else if (statuses.some((s) => s === "triggered" || s === "completed")) {
    overallStatus = "triggered";
  }

  ingestion.status = overallStatus;
  res.json({
    ingestion_id: req.params.id,
    status: overallStatus,
    batches: ingestion.batches,
  });
});

// executing the batch
setInterval(async () => {
  if (batchQueue.length === 0) return;

  batchQueue.sort((a, b) => {
    if (PRIORITY_MAP[a.priority] === PRIORITY_MAP[b.priority]) {
      return a.created_time - b.created_time;
    }
    return PRIORITY_MAP[a.priority] - PRIORITY_MAP[b.priority];
  });

  const batch = batchQueue.shift();
  batch.status = "triggered";

  console.log(
    `Processing batch ${batch.batch_id}:`,
    batch.ids
  );

  batch.status = "completed";
  console.log(
    `Completed batch ${batch.batch_id}`
  );
}, 5000); 

app.listen(PORT, () =>
  console.log(`Server running on http://localhost:${PORT}`)
);

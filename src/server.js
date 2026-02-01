// src/server.js
import express from "express";
import jobsRoutes from "./routes/jobs.js";

const app = express();
app.use(express.json());

app.use("/jobs", jobsRoutes);

app.listen(3000, () => {
  console.log("Server running on port 3000");
});

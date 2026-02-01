// src/db.js
import {Pool} from "pg";

const pool = new Pool({
  host: "localhost",
  port: 5432,
  user: "postgres",
  password: "chandu132",
  database: "postgres"
});

export default {
  query: (text, params) => pool.query(text, params)
};


import os
import threading
import datetime
from flask import Flask, jsonify, Response
import queue

app = Flask(__name__)

# In-memory log store for streaming
log_queue = queue.Queue()
etl_running = False
etl_logs = []
last_run = None
last_status = None

def log(msg):
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    etl_logs.append(line)
    print(line)

def run_etl_pipeline():
    global etl_running, last_run, last_status
    etl_logs.clear()
    etl_running = True
    last_run = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        log("=== ETL Pipeline Started ===")

        # --- EXTRACT ---
        log("▶ EXTRACT: Loading CSVs into staging schema...")
        import pandas as pd
        from sqlalchemy import create_engine, text
        from pathlib import Path

        DATABASE_URL = os.environ["DATABASE_URL"]
        SOURCE_PATH = Path("data/source")

        def get_engine():
            return create_engine(DATABASE_URL)

        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
            conn.commit()

        for store_name, prefix in [("japan_store", "japan"), ("myanmar_store", "myanmar")]:
            store_path = SOURCE_PATH / store_name
            for csv_file in store_path.glob("*.csv"):
                table_name = f"{prefix}_{csv_file.stem.lower().replace(' ', '_')}"
                df = pd.read_csv(csv_file)
                df.to_sql(table_name, engine, schema="staging", if_exists="replace", index=False)
                log(f"  Loaded {csv_file.name} → staging.{table_name}")
        engine.dispose()
        log("✔ EXTRACT complete.")

        # --- TRANSFORM ---
        log("▶ TRANSFORM: Cleaning and merging data...")
        JPY_PER_USD = 150
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS transformation"))
            conn.commit()

        for country in ["japan", "myanmar"]:
            sales = pd.read_sql(f"SELECT * FROM staging.{country}_sales_data", engine)
            items = pd.read_sql(f"SELECT * FROM staging.{country}_{country}_items", engine)
            sales.columns = sales.columns.str.replace("'", "").str.lower()
            items.columns = items.columns.str.lower()
            sales.dropna(inplace=True)
            items.dropna(inplace=True)
            df = sales.merge(items, left_on="product_id", right_on="id", how="left")
            df["price_usd"] = df["price"] / JPY_PER_USD if country == "japan" else df["price"]
            df["country"] = country.capitalize()
            df.to_sql(f"{country}_transformed", engine, schema="transformation", if_exists="replace", index=False)
            log(f"  Transformed {country} → transformation.{country}_transformed ({len(df)} rows)")
        engine.dispose()
        log("✔ TRANSFORM complete.")

        # --- LOAD ---
        log("▶ LOAD: Building consolidated presentation table...")
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS presentation"))
            conn.commit()

        japan = pd.read_sql("SELECT * FROM transformation.japan_transformed", engine)
        myanmar = pd.read_sql("SELECT * FROM transformation.myanmar_transformed", engine)
        big_table = pd.concat([japan, myanmar], ignore_index=True)
        big_table.to_sql("consolidated_sales", engine, schema="presentation", if_exists="replace", index=False)
        engine.dispose()
        log(f"  Created presentation.consolidated_sales ({len(big_table)} rows)")
        log("✔ LOAD complete.")

        log("=== ETL Pipeline Finished Successfully! ===")
        last_status = "success"

    except Exception as e:
        log(f"✘ ETL FAILED: {str(e)}")
        last_status = "error"
    finally:
        etl_running = False


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>ETL Pipeline — Sevilla ADBMS</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', system-ui, sans-serif;
      background: #0f1117;
      color: #e2e8f0;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 48px 16px;
    }
    .card {
      background: #1a1d2e;
      border: 1px solid #2d3148;
      border-radius: 16px;
      padding: 40px;
      width: 100%;
      max-width: 700px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.4);
    }
    h1 {
      font-size: 1.6rem;
      font-weight: 700;
      margin-bottom: 4px;
      color: #fff;
    }
    .subtitle {
      color: #7c85a2;
      font-size: 0.9rem;
      margin-bottom: 32px;
    }
    .pipeline {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      margin-bottom: 32px;
    }
    .step {
      background: #252840;
      border: 1px solid #3a3f5c;
      border-radius: 10px;
      padding: 12px 20px;
      text-align: center;
      font-size: 0.8rem;
      font-weight: 600;
      color: #a0aec0;
      min-width: 90px;
    }
    .step span { display: block; font-size: 1.3rem; margin-bottom: 4px; }
    .arrow { color: #4a5080; font-size: 1.2rem; }
    .btn {
      display: block;
      width: 100%;
      padding: 16px;
      border: none;
      border-radius: 10px;
      font-size: 1rem;
      font-weight: 700;
      cursor: pointer;
      transition: all 0.2s;
      background: linear-gradient(135deg, #6366f1, #8b5cf6);
      color: #fff;
      letter-spacing: 0.5px;
    }
    .btn:hover:not(:disabled) {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(99,102,241,0.4);
    }
    .btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
    }
    .status-bar {
      display: flex;
      align-items: center;
      gap: 10px;
      margin: 20px 0 12px;
      font-size: 0.85rem;
      color: #7c85a2;
      min-height: 22px;
    }
    .dot {
      width: 10px; height: 10px;
      border-radius: 50%;
      background: #3a3f5c;
      flex-shrink: 0;
    }
    .dot.running { background: #f59e0b; animation: pulse 1s infinite; }
    .dot.success { background: #10b981; }
    .dot.error   { background: #ef4444; }
    @keyframes pulse {
      0%,100% { opacity: 1; } 50% { opacity: 0.3; }
    }
    .log-box {
      background: #0d0f1a;
      border: 1px solid #2d3148;
      border-radius: 10px;
      padding: 16px;
      font-family: 'Courier New', monospace;
      font-size: 0.78rem;
      color: #a0aec0;
      height: 280px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-all;
    }
    .log-box .ok   { color: #10b981; }
    .log-box .err  { color: #ef4444; }
    .log-box .info { color: #6366f1; }
    .log-box .dim  { color: #4a5080; }
    .meta { margin-top: 16px; font-size: 0.78rem; color: #4a5080; text-align: right; }
  </style>
</head>
<body>
  <div class="card">
    <h1>ETL Pipeline</h1>
    <p class="subtitle">Sevilla ADBMS Activity &mdash; PostgreSQL on Render</p>

    <div class="pipeline">
      <div class="step">Extract</div>
      <div class="arrow">→</div>
      <div class="step">Transform</div>
      <div class="arrow">→</div>
      <div class="step">Load</div>
    </div>

    <button class="btn" id="runBtn" onclick="runETL()">▶ Run ETL Pipeline</button>

    <div class="status-bar">
      <div class="dot" id="dot"></div>
      <span id="statusText">Ready to run.</span>
    </div>

    <div class="log-box" id="logBox">Logs will appear here...</div>
    <div class="meta" id="meta"></div>
  </div>

  <script>
    let polling = null;

    function colorize(line) {
      if (line.includes('✔') || line.includes('Successfully') || line.includes('Finished'))
        return `<span class="ok">${line}</span>`;
      if (line.includes('✘') || line.includes('FAILED') || line.includes('Error'))
        return `<span class="err">${line}</span>`;
      if (line.includes('==='))
        return `<span class="info">${line}</span>`;
      if (line.includes('Loaded') || line.includes('Transformed') || line.includes('Created'))
        return `<span class="dim">${line}</span>`;
      return line;
    }

    function runETL() {
      document.getElementById('runBtn').disabled = true;
      document.getElementById('dot').className = 'dot running';
      document.getElementById('statusText').textContent = 'ETL running...';
      document.getElementById('logBox').innerHTML = '';
      document.getElementById('meta').textContent = '';

      fetch('/run', { method: 'POST' })
        .then(r => r.json())
        .then(data => {
          if (data.status === 'started' || data.status === 'already_running') {
            polling = setInterval(fetchLogs, 1000);
          }
        })
        .catch(() => {
          document.getElementById('statusText').textContent = 'Failed to start ETL.';
          document.getElementById('dot').className = 'dot error';
          document.getElementById('runBtn').disabled = false;
        });
    }

    function fetchLogs() {
      fetch('/status')
        .then(r => r.json())
        .then(data => {
          const box = document.getElementById('logBox');
          box.innerHTML = data.logs.map(colorize).join('\\n');
          box.scrollTop = box.scrollHeight;

          if (!data.running) {
            clearInterval(polling);
            document.getElementById('runBtn').disabled = false;
            const dot = document.getElementById('dot');
            const st  = document.getElementById('statusText');
            if (data.last_status === 'success') {
              dot.className = 'dot success';
              st.textContent = 'Pipeline completed successfully!';
            } else if (data.last_status === 'error') {
              dot.className = 'dot error';
              st.textContent = 'Pipeline failed. Check logs.';
            } else {
              dot.className = 'dot';
              st.textContent = 'Ready.';
            }
            if (data.last_run)
              document.getElementById('meta').textContent = 'Last run: ' + data.last_run;
          }
        });
    }
  </script>
</body>
</html>
"""

@app.route("/")
def index():
    return Response(HTML_PAGE, mimetype="text/html")

@app.route("/run", methods=["POST"])
def run():
    global etl_running
    if etl_running:
        return jsonify({"status": "already_running"})
    t = threading.Thread(target=run_etl_pipeline, daemon=True)
    t.start()
    return jsonify({"status": "started"})

@app.route("/status")
def status():
    return jsonify({
        "running": etl_running,
        "logs": etl_logs,
        "last_run": last_run,
        "last_status": last_status,
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

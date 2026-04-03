# MLOps Batch Job - Technical Assessment
#Done by: Lisa Hazel Dsouza
A minimal MLOps-ready batch job built for signal generation on OHLCV data. This job demonstrates reproducibility, observability, and deployment readiness.

## Project Structure
- `run.py`: Core processing script.
- `config.yaml`: Job configuration (`seed`, `window`, `version`).
- `data.csv`: Input OHLCV dataset (10,000 rows).
- `requirements.txt`: Python dependencies.
- `Dockerfile`: Container image definition.

## ⚙️ Setup

### Download the Repository
If you are starting from scratch, clone the repository:
```bash
git clone https://github.com/LisaxDsouza/MLOPs-PrimeTradeAI-Assigment.git
cd MLOPs-PrimeTradeAI-Assigment
```

### Local Environment
1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  Run the pipeline:
    ```bash
    python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
    ```

### Dockerized Run
1.  Build the image:
    ```bash
    docker build -t mlops-task .
    ```
2.  Run the container:
    ```bash
    docker run --rm mlops-task
    ```

## 📊 Example metrics.json
```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.5008,
  "latency_ms": 17,
  "seed": 42,
  "status": "success"
}
```

## 📝 Observability
- **run.log**: Stores job execution steps, configuration validation, and processing updates.
- **metrics.json**: Captured in both success and failure cases to provide a machine-readable summary.
- **Exit Codes**: Returns `0` on success and `1` on data or configuration errors.

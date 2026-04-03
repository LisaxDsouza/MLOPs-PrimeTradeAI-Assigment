import argparse
import yaml
import pandas as pd
import numpy as np
import json
import logging
import time
import os
import sys

def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def write_metrics(output_file, metrics):
    with open(output_file, 'w') as f:
        json.dump(metrics, f, indent=2)

def main():
    start_time = time.perf_counter()
    
    parser = argparse.ArgumentParser(description="MLOps Batch Job Execution")
    parser.add_argument("--input", required=True, help="Path to input data.csv")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--output", required=True, help="Path to metrics.json")
    parser.add_argument("--log-file", required=True, help="Path to run.log")
    
    args = parser.parse_args()
    
    setup_logging(args.log_file)
    logging.info("Job start timestamp: %s", time.strftime("%Y-%m-%d %H:%M:%S"))
    
    version = "unknown"
    seed = None
    
    try:
        # Load and validate config
        if not os.path.exists(args.config):
            raise FileNotFoundError(f"Config file not found at: {args.config}")
            
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
            
        required_config = ['seed', 'window', 'version']
        for field in required_config:
            if field not in config:
                raise KeyError(f"Missing required config field: {field}")
        
        seed = config['seed']
        window = config['window']
        version = config['version']
        
        np.random.seed(seed)
        logging.info("Config loaded and validated (seed=%d, window=%d, version=%s)", seed, window, version)
        
        # Load and validate dataset
        if not os.path.exists(args.input):
            raise FileNotFoundError(f"Input file not found at: {args.input}")
            
        df = pd.read_csv(args.input)
        
        if df.empty:
            raise ValueError("Input CSV is empty")
            
        if 'close' not in df.columns:
            raise KeyError("Missing required column: close")
            
        rows_processed = len(df)
        logging.info("Rows loaded: %d", rows_processed)
        
        # Processing steps
        logging.info("Computing rolling mean (window=%d)", window)
        # Handle the first window-1 rows (NaNs)
        df['rolling_mean'] = df['close'].rolling(window=window).mean()
        
        logging.info("Generating signals")
        # signal = 1 if close > rolling_mean, else 0
        df['signal'] = np.where(df['rolling_mean'].isna(), np.nan, (df['close'] > df['rolling_mean']).astype(int))
        
        # signal_rate = mean(signal) excluding NaNs
        signal_rate_val = df['signal'].dropna().mean()
        
        end_time = time.perf_counter()
        latency_ms = int((end_time - start_time) * 1000)
        
        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(float(signal_rate_val), 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }
        
        logging.info("Metrics summary: %s", json.dumps(metrics))
        write_metrics(args.output, metrics)
        print(json.dumps(metrics))
        logging.info("Job end - Success")
        
    except Exception as e:
        error_msg = str(e)
        logging.error("Fatal error during job: %s", error_msg)
        error_metrics = {
            "version": version,
            "status": "error",
            "error_message": error_msg
        }
        write_metrics(args.output, error_metrics)
        # print error metric to stdout as well
        print(json.dumps(error_metrics))
        sys.exit(1)

if __name__ == "__main__":
    main()

import os
import json
import uuid
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine
from confluent_kafka import Consumer
from dotenv import load_dotenv

# 1. LOAD CREDENTIALS
load_dotenv(dotenv_path='Credentials.env.sh')

# --- CONFIGURATION / MAPPING CONTROL PANEL ---
EQP_NAMES_STACK = ["model_name"] 

# Mapping:
MAP_EQUIPMENT = {
    "street_name": "city",
    "city": "city",
    "equipment_id": "equipment_id",
  #  "serial_number": 
}

def read_kafka_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#" and "=" in line:
                parameter, value = line.split('=', 1)
                config[parameter.strip()] = value.strip()
    return config

def main():
    # --- 2. DATABASE CONNECTION ---
    pg_pass = urllib.parse.quote_plus(os.getenv('PG_PASSWORD'))
    # pg_pass = os.getenv('PG_PASSWORD')
    pg_url = f"postgresql+psycopg2://{os.getenv('PG_USER')}:{pg_pass}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    pg_engine = create_engine(pg_url)

    # --- 3. KAFKA SETUP ---
    kafka_config = read_kafka_config()
    kafka_config.update({
        "group.id": "prism-consumer-group-v2", # Changed name to reset position if needed
        "auto.offset.reset": "earliest"
    })
    
    consumer = Consumer(kafka_config)
    consumer.subscribe(["Mysql_data"])

    print("PRISM Consumer Online. Listening for Kafka events...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer Error: {msg.error()}")
                continue

            # --- 4. DATA EXTRACTION WITH SAFETY WRAPPER ---
            raw_value = msg.value().decode('utf-8')
            
            try:
                # This is where the "Oleg" and "key/value" messages were failing
                raw_payload = json.loads(raw_value)
            except json.JSONDecodeError:
                print(f"--- SKIPPING NON-JSON MESSAGE: {raw_value} ---")
                continue

            # --- 5. TRANSFORMATION & STACKING ---
            df_source = pd.DataFrame([raw_payload])
            valid_stack_cols = [col for col in EQP_NAMES_STACK if col in df_source.columns]
            
            df_target = pd.DataFrame()
            df_target["model_name"] = pd.concat([df_source[col] for col in valid_stack_cols], ignore_index=True)
            
            num_rows = len(df_target)

            for json_key, db_col in MAP_EQUIPMENT.items():
                if json_key in df_source.columns:
                    df_target[db_col] = pd.concat([df_source[json_key]] * num_rows, ignore_index=True)

            # Build Full Address
            street = raw_payload.get('street_name', 'Unknown')
            city = raw_payload.get('city', 'Unknown')
            # df_target["full_address"] = f"{street}, {city}"
            df_target["processed_at"] = pd.Timestamp.now()
           # df_target["id"] = [str(uuid.uuid4()) for _ in range(num_rows)]

            # --- 6. LOAD TO POSTGRES ---
            df_target.to_sql(
                name="olt",
                con=pg_engine,
                schema="etl_in",
                if_exists='append',
                index=False,
                method='multi'
            )

            print(f"Successfully processed: {raw_payload.get('serial_number')}")

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()
        pg_engine.dispose()

if __name__ == "__main__":
    main()
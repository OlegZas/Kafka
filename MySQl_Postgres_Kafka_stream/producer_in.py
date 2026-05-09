import os
import json
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load credentials from your .env file
load_dotenv(dotenv_path='Credentials.env.sh')

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
    # 1. Setup Kafka Producer
    kafka_config = read_kafka_config()
    producer = Producer(kafka_config)
    topic = "Mysql_data"

    # 2. DATABASE CONNECTION (SQLAlchemy - matching your Postgres style)
    print("Connecting to MySQL...")
    
    # URL encode the password just in case it has special characters
    mysql_pass = urllib.parse.quote_plus(os.getenv('MYSQL_PASSWORD'))
    
    # Connection string for MySQL via SQLAlchemy
    mysql_url = f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:{mysql_pass}@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
    
    # Create the engine
    mysql_engine = create_engine(mysql_url)

    # 3. LOAD DATA (Using Pandas directly from the Database!)
    query = """
    select e.equipment_id, e.model_name, e.serial_number, e.address_id,sa.street_name , sa.city 
    from etl_out.equipment e 
    inner join etl_out.service_address sa on e.address_id = sa.address_id 
    """
    
    # pd.read_sql is the exact opposite of df.to_sql
    print("Extracting data via Pandas...")
    df_source = pd.read_sql(query, con=mysql_engine)

    # 4. Push one-by-one to Kafka
    print(f"Found {len(df_source)} records. Pushing to Kafka...")
    
    # Convert DataFrame back to a list of dictionaries for JSON
    records = df_source.to_dict(orient='records')
    
    for row in records:
        json_payload = json.dumps(row)
        
        # We use serial_number as the Kafka Key to ensure order
        producer.produce(topic, key=str(row['serial_number']), value=json_payload)
        
    producer.flush()
    print("SUCCESS: All records pushed to Kafka.")

    # Clean up the connection
    mysql_engine.dispose()

if __name__ == "__main__":
    main()
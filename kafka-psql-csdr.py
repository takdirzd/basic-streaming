import json
import logging
import requests
import time
import uuid
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Configuration
KAFKA_TOPIC = "users_created"
KAFKA_BROKER = "localhost:9092"

# PostgreSQL Configuration
PG_HOST = "localhost"
PG_DB = "my_db"
PG_USER = "postgres"
PG_PASS = "postgres"
PG_SCHEMA = "stream"

# Cassandra Configuration
CASSANDRA_HOST = "localhost"
CASSANDRA_KEYSPACE = "streams"
CASSANDRA_TABLE = "created_users"

# API Configuration
API_URL = "https://randomuser.me/api/"

# =========================
# 1. FETCH DATA FROM API
# =========================
def fetch_user_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()["results"][0]
        user_data = {
            "id": str(uuid.uuid4()),  # Generate UUID untuk PostgreSQL & Cassandra
            "first_name": data["name"]["first"],
            "last_name": data["name"]["last"],
            "gender": data["gender"],
            "address": f"{data['location']['street']['number']} {data['location']['street']['name']}, {data['location']['city']}, {data['location']['state']}",
            "post_code": str(data["location"]["postcode"]),
            "email": data["email"],
            "username": data["login"]["username"],
            "registered_date": data["registered"]["date"],
            "phone": data["phone"],
            "picture": data["picture"]["large"]
        }
        return user_data
    else:
        logging.error("Failed to fetch user data from API")
        return None

# =========================
# 2. INSERT TO POSTGRE
# =========================
def connect_postgres():
    try:
        conn = psycopg2.connect(
            host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS
        )
        logging.info("Connected to PostgreSQL successfully!")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        return None



def insert_into_postgres(conn, data):
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {PG_SCHEMA};")
            cur.execute(f"""
                INSERT INTO {PG_SCHEMA}.users_created (id, first_name, last_name, gender, address, post_code, email, 
                                   username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data["id"], data["first_name"], data["last_name"], data["gender"],
                data["address"], data["post_code"], data["email"], data["username"],
                data["registered_date"], data["phone"], data["picture"]
            ))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Failed to insert into PostgreSQL: {e}")
        return False

# =========================
# 3. SETUP KAFKA PRODUCER (for Cassandra)
# =========================
def produce_message(producer, user_data):
    producer.send(KAFKA_TOPIC, user_data)
    logging.info(f"Produced to Kafka: {user_data['email']}")

# =========================
# 4️. INSERT TO CASSANDRA
# =========================
def connect_to_cassandra():
    try:
        # mencegah warning yg muncul dari cassandra
        cluster = Cluster(
            contact_points=[CASSANDRA_HOST],
            protocol_version=5,  # Sesuaikan dengan versi Cassandra
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')  # Sesuaikan jika perlu
        )
        session = cluster.connect()
        session.set_keyspace(CASSANDRA_KEYSPACE)
        logging.info("Connected to Cassandra successfully!")
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        return None

def insert_into_cassandra(session, data):
    try:
        query = f"""
            INSERT INTO {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (
                id, first_name, last_name, gender, address, post_code, email, 
                username, registered_date, phone, picture
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        statement = session.prepare(query)

        # Konversi id ke UUID
        cassandra_id = uuid.UUID(data["id"]) if "id" in data else uuid.uuid4()

        session.execute(statement, (
            cassandra_id, data["first_name"], data["last_name"], data["gender"],
            data["address"], data["post_code"], data["email"], data["username"],
            data["registered_date"], data["phone"], data["picture"]
        ))
        
        return True
    except Exception as e:
        logging.error(f"Failed to insert into Cassandra: {e}")
        return False

# =========================
# 5. KAFKA CONSUMER (for Cassandra)
# =========================
def consume_messages():
    session = connect_to_cassandra()
    if not session:
        return

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
    )

    for message in consumer:
        data = message.value
        logging.info(f"Received from Kafka: {data['email']}")

        if insert_into_cassandra(session, data):
            logging.info(f"Inserted into Cassandra: {data['email']}")
        else:
            logging.error("Failed to insert into Cassandra.")

# =========================
# 6. MAIN FUNCTION
# =========================
if __name__ == "__main__":
    conn = connect_postgres()
    if not conn:
        exit()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    logging.info("Starting data ingestion...")

    start_time = time.time()
    while time.time() - start_time < 60:  # Get data for 1 minute
        user_data = fetch_user_data()
        if user_data:
            if insert_into_postgres(conn, user_data):  # Insert into PostgreSQL
                produce_message(producer, user_data)  # Insert from Kafka to Cassandra

    producer.flush()
    producer.close(timeout=10)
    logging.info("Stopping producer.")

    consume_messages() 

import snowflake.connector
from cryptography.hazmat.primitives import serialization

# ========= CONFIG =========
ACCOUNT   = "your_account"      # e.g. xy12345.eu-west-1
USER      = "your_service_user"
WAREHOUSE = "your_warehouse"
DATABASE  = "your_database"
SCHEMA    = "public"
ROLE      = "your_role"
PRIVATE_KEY_PATH = "private_key.p8"
PRIVATE_KEY_PASSWORD = None  # set b"password" if encrypted
# ===========================


def load_private_key():
    with open(PRIVATE_KEY_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=PRIVATE_KEY_PASSWORD
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


try:
    print("Loading private key...")
    pkb = load_private_key()

    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        private_key=pkb,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role=ROLE
    )

    print("Connected successfully ✅")

    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION()")
    result = cursor.fetchone()

    print("User:", result[0])
    print("Role:", result[1])
    print("Snowflake Version:", result[2])

    cursor.close()
    conn.close()

except Exception as e:
    print("Connection failed ❌")
    print("Error:", e)
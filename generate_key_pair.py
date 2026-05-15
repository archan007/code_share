"""
Generate RSA key pair for Snowflake key-pair authentication from Qlik.

Outputs:
    - rsa_key.p8         : Encrypted private key in PKCS#8 format
    - rsa_key.pub        : Public key in PEM format (to be registered in Snowflake)
    - passphrase.txt     : Passphrase used to encrypt the private key

Requirements:
    pip install cryptography

Usage:
    python generate_snowflake_keys.py
"""

import os
import secrets
import string
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


# ---------- Configuration ----------
OUTPUT_DIR = Path("./snowflake_keys")
PRIVATE_KEY_FILE = "rsa_key.p8"
PUBLIC_KEY_FILE = "rsa_key.pub"
PASSPHRASE_FILE = "passphrase.txt"
KEY_SIZE = 2048           # Snowflake recommends 2048-bit minimum
PASSPHRASE_LENGTH = 24    # Length of auto-generated passphrase
# -----------------------------------


def generate_passphrase(length: int = PASSPHRASE_LENGTH) -> str:
    """Generate a cryptographically strong random passphrase."""
    # Avoid characters that can cause issues in config files / shell (quotes, backslash)
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*-_=+?"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def generate_key_pair(passphrase: str, output_dir: Path) -> None:
    """Generate RSA key pair and write to disk."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Generate the RSA private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=KEY_SIZE,
    )

    # 2. Serialize the private key in encrypted PKCS#8 PEM format
    #    This is the format Qlik / Snowflake require.
    encrypted_private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(
            passphrase.encode("utf-8")
        ),
    )

    private_key_path = output_dir / PRIVATE_KEY_FILE
    private_key_path.write_bytes(encrypted_private_pem)
    # Tighten permissions on the private key (POSIX systems)
    try:
        os.chmod(private_key_path, 0o600)
    except (OSError, NotImplementedError):
        pass  # Windows or restricted FS - ignore

    # 3. Serialize the public key in SubjectPublicKeyInfo (PEM) format
    public_key = private_key.public_key()
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    public_key_path = output_dir / PUBLIC_KEY_FILE
    public_key_path.write_bytes(public_pem)

    # 4. Save the passphrase to a text file
    passphrase_path = output_dir / PASSPHRASE_FILE
    passphrase_path.write_text(passphrase, encoding="utf-8")
    try:
        os.chmod(passphrase_path, 0o600)
    except (OSError, NotImplementedError):
        pass

    # 5. Extract the bare public key body (without PEM headers).
    #    This is the value to paste into Snowflake:
    #    ALTER USER <user> SET RSA_PUBLIC_KEY='<this string>';
    public_key_body = (
        public_pem.decode("utf-8")
        .replace("-----BEGIN PUBLIC KEY-----", "")
        .replace("-----END PUBLIC KEY-----", "")
        .replace("\n", "")
        .strip()
    )

    print("✅ Key pair generated successfully.\n")
    print(f"  Private key (PKCS#8, encrypted) : {private_key_path.resolve()}")
    print(f"  Public key  (PEM)               : {public_key_path.resolve()}")
    print(f"  Passphrase file                 : {passphrase_path.resolve()}")
    print("\nNext steps for Snowflake:")
    print("  Run the following in Snowflake to register the public key for your user:\n")
    print(f"  ALTER USER <your_user> SET RSA_PUBLIC_KEY='{public_key_body}';\n")
    print("Then in Qlik:")
    print("  - Upload/point to the private key file (rsa_key.p8)")
    print("  - Provide the passphrase from passphrase.txt when prompted")


def main() -> None:
    passphrase = generate_passphrase()
    generate_key_pair(passphrase, OUTPUT_DIR)


if __name__ == "__main__":
    main()
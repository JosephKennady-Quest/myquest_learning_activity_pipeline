# config.example.py
# Copy this file to config.py and fill in your actual values.
# NEVER commit config.py to version control.

CONFIG = {
    "source": {
        "ssh": {
            "host": "YOUR_SOURCE_BASTION_HOST_IP",
            "port": 22,
            "username": "YOUR_SSH_USERNAME",
            "pkey_path": "DB_Config/your_key.pem",        # relative path from project root
            "remote_bind_address": "YOUR_RDS_ENDPOINT.rds.amazonaws.com",
            "remote_bind_port": 3306
        },
        "db": {
            "user": "YOUR_DB_USER",
            "password": "YOUR_DB_PASSWORD",
            "database": "YOUR_DATABASE_NAME"
        }
    },
    "destination": {
        "ssh": {
            "host": "YOUR_DEST_BASTION_HOST_IP",
            "port": 22,
            "username": "YOUR_SSH_USERNAME",
            "pkey_path": "DB_Config/your_dest_key.pem",
            "remote_bind_address": "YOUR_DEST_RDS_ENDPOINT.rds.amazonaws.com",
            "remote_bind_port": 3306
        },
        "db": {
            "user": "YOUR_DEST_DB_USER",
            "password": "YOUR_DEST_DB_PASSWORD",
            "database": "YOUR_DEST_DATABASE_NAME"
        }
    }
}

CHUNK_SIZE = 300

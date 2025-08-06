import sqlite3
def get_connection(db_path="input/sales.db"):
    return sqlite3.connect(db_path)

import sqlite3
import pandas as pd

def etl_sql(db_path, output_path):
    conn = sqlite3.connect(db_path)
    query = """
        SELECT 
            c.customer_id AS customer,
            c.age AS age,
            i.item_name AS item,
            SUM(oi.quantity) AS quantity
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN items i ON oi.item_id = i.item_id
        WHERE c.age BETWEEN 18 AND 35
          AND oi.quantity IS NOT NULL
        GROUP BY c.customer_id, i.item_name
        HAVING SUM(oi.quantity) > 0
        ORDER BY c.customer_id;
	"""
    df = pd.read_sql_query(query, conn)
    df.to_csv(output_path, sep=';', index=False)
    conn.close()

if __name__ == "__main__":
    etl_sql("input/sales.db", "output/item_summary.csv")

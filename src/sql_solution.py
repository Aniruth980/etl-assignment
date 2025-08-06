import sqlite3
import pandas as pd

def etl_sql(db_path, output_path):
    conn = sqlite3.connect(db_path)
    query = """
        SELECT 
            c.customer_id AS customer,
            c.age AS age,
            i.item_name AS item,
            SUM(o.quantity) AS quantity
        FROM customers c
        JOIN sales s ON c.customer_id = s.customer_id
        JOIN orders o ON s.sales_id = o.sales_id
        JOIN items i ON o.item_id = i.item_id
        WHERE c.age BETWEEN 18 AND 35
          AND o.quantity IS NOT NULL
        GROUP BY c.customer_id, i.item_name
        HAVING SUM(o.quantity) > 0
        ORDER BY c.customer_id;
	"""
    df = pd.read_sql_query(query, conn)
    df.to_csv(output_path, sep=';', index=False)
    conn.close()

if __name__ == "__main__":
    etl_sql("input/sales.db", "output/item_summary.csv")

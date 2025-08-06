import sqlite3
import pandas as pd

def etl_pandas(db_path, output_path):
    conn = sqlite3.connect(db_path)

    customers = pd.read_sql("SELECT * FROM customers", conn)
    orders = pd.read_sql("SELECT * FROM orders", conn)
    order_items = pd.read_sql("SELECT * FROM order_items", conn)
    items = pd.read_sql("SELECT * FROM items", conn)

    joined_table = orders.merge(customers, on='customer_id') \
                   .merge(order_items, on='order_id') \
                   .merge(items, on='item_id')

    filter_table = joined_table[(joined_table['age'].between(18, 35)) & (joined_table['quantity'].notnull())]
    groupby_table = filter_table.groupby(['customer_id', 'age', 'item_name'])['quantity'].sum().reset_index()
    result = groupby_table[groupby_table['quantity'] > 0]

    result.columns = ['Customer', 'Age', 'Item', 'Quantity']
    result.to_csv(output_path, sep=';', index=False)

    conn.close()

if __name__ == "__main__":
    etl_pandas("input/sales.db", "output/item_summary.csv")

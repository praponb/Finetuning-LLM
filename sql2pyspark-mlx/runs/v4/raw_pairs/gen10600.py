from pyspark.sql import functions as F

result = df_orders.select("customer_id", "channel", "order_date")

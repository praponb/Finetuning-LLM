from pyspark.sql import functions as F

result = df_customers.select("id").subtract(df_orders.select("customer_id"))

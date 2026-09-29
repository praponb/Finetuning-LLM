from pyspark.sql import functions as F

result = df_orders.filter((F.col("amount") <= 1020) & F.month("order_date").isin([2, 5, 7]))

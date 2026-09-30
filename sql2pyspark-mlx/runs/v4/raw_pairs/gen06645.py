from pyspark.sql import functions as F

result = df_shipments.filter((F.col("cost") <= 92) & F.col("order_id").isNull())

from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date").between("2024-11-01", "2025-01-01") & (F.col("shipping_fee") <= 20)) | (F.col("channel") == "partner"))
    .withColumn("order_date_month_end", F.last_day("order_date"))
)

from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 36) & F.col("store_id").isin([17, 31, 40]))
    .orderBy(F.col("shipping_fee").desc(), "id")
    .limit(10)
)

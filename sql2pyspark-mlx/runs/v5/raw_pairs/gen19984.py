from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() & (F.col("store_id").isin([15, 31, 38]) | F.col("store_id").isNotNull()))
    .select("shipping_fee", "discount", "status")
    .orderBy(F.col("shipping_fee").desc())
)

from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select("order_date", F.lit("order").alias("source"))
    .union(
        df_inventory
        .filter(F.col("store_id") == 3)
        .select("last_restock", F.lit("inventory").alias("source"))
    )
)

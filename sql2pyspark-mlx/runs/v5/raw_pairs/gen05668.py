from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("order_date", F.lit("order").alias("origin"))
    .union(
        df_inventory
        .select("last_restock", F.lit("inventory").alias("origin"))
    )
    .distinct()
)

from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.lit("order").alias("origin"), F.col("order_date").alias("event_date"))
    .unionByName(
        df_inventory
        .select(
        F.lit("inventory").alias("origin"),
        F.col("last_restock").alias("event_date"),
    )
    )
    .distinct()
)

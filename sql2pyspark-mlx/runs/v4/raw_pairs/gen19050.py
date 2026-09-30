from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount", F.lit("order").alias("source"))
    .unionByName(
        df_order_items
        .select(F.col("unit_price").alias("amount"), F.lit("order_item").alias("source"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)

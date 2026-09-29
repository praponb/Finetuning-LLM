from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-03-25").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        F.coalesce("discount", F.lit(0)).alias("discount_filled_2"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.store_name"))
)

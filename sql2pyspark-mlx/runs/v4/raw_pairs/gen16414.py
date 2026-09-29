from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-10-03").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.date_trunc("week", "order_date").alias("order_date_period"),
        F.when(F.col("amount") >= 1600, "High").when(F.col("amount") >= 680, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.city"))
)

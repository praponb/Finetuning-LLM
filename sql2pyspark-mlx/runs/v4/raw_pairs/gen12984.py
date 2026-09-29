from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-06-26").cast("date"))
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.date_format("order_date", "MMM yyyy").alias("order_date_label"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .groupBy("store_id", "status")
    .pivot("channel", ["store", "mobile"])
    .agg(F.avg("amount").alias("total"), F.count(F.lit(1)).alias("cnt"))
)

from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-03-31").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.date_add("order_date", 67).alias("order_date_plus_67d"),
    )
)

result = (
    cleaned
    .groupBy("channel")
    .pivot("status", ["CANCELLED", "SHIPPED"])
    .agg(F.avg("amount"))
)

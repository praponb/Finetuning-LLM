from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.month("order_date").isin([2, 11, 12]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    cleaned
    .groupBy("status")
    .pivot("channel", ["partner", "web"])
    .agg(F.avg("amount_times_discount"))
)

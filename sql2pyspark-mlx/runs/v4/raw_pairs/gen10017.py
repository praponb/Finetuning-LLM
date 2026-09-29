from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-08-27").cast("date"))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.trim("channel").alias("clean_channel"),
        (F.col("discount") * F.col("amount")).alias("discount_times_amount"),
    )
)

result = (
    base
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") <= F.col("s.order_date")), "left_anti")
)

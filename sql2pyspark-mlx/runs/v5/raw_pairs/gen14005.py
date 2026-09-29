from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-05-01")))
    .select(
        "*",
        (F.col("shipping_fee") * F.col("discount")).alias("shipping_fee_times_discount"),
        F.length("status").alias("status_length"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    valid
    .select("id")
    .intersect(
        df_orders
        .filter(~F.col("channel").isin(["store", "mobile"]))
        .select("id")
    )
)

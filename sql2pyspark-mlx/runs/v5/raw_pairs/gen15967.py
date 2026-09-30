from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "SHIPPED") & (F.col("channel") != "web"))
    .select(
        "id",
        "channel",
        "amount",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)

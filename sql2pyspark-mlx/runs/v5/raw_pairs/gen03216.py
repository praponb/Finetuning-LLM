from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "mobile")
    .select(
        "discount",
        "amount",
        "store_id",
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
    .orderBy("store_id")
)

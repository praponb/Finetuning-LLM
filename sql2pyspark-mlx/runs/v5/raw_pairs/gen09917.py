from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "UPS") & F.col("order_id").isin([2, 3, 8, 30]))
    .select(
        "shipped_date",
        "order_id",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
    .orderBy("shipped_date", F.col("status_prefix").desc())
)

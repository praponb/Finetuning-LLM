from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-08-01", "2024-04-01") & F.col("discount").between(0.18, 0.33))
    .select(
        "id",
        "status",
        "shipping_fee",
        F.when(F.col("channel").isin(["store", "web"]), "Tier 1").otherwise("Other").alias("channel_group"),
    )
    .orderBy("status", F.col("shipping_fee").desc())
)

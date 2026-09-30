from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        F.when(F.col("carrier").isin(["Kerry", "Flash"]), "Tier 1").otherwise("Other").alias("carrier_group"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

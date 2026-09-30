from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") <= 95) & (F.col("status") == "LOST"))
    .select(
        "id",
        "cost",
        "delivered_date",
        "shipped_date",
        F.round("cost", 0).alias("cost_rounded"),
        F.concat_ws("/", "status", "carrier").alias("status_carrier_key"),
    )
    .orderBy(F.col("cost").desc())
)

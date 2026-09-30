from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "Kerry"]) & (F.col("cost").isNotNull() | (F.col("cost") >= 95)))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
    )
)

from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("cost") <= 26) & F.col("status").isin(["LOST", "RETURNED"]))
)

summary = (
    recent_shipments
    .groupBy(
        F.when(F.col("cost") >= 98, "high").when(F.col("cost") >= 36, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
    )
)

result = summary.filter(F.col("dhl_cost") >= 63)

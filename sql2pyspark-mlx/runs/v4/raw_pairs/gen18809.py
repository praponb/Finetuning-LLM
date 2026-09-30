from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "DHL", "Flash"]) & (F.col("cost") > 46) & F.col("cost").between(66, 85))
    .groupBy(
        F.when(F.col("cost") >= 80, "high").when(F.col("cost") >= 30, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

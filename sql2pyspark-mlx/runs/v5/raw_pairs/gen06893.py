from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.col("shipped_date").between("2024-06-01", "2024-08-01") & (F.col("status") != "LOST")) | F.col("carrier").isin(["DHL", "Flash", "Kerry"])))
    .groupBy(
        F.when(F.col("cost") >= 105, "high").when(F.col("cost") >= 45, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .orderBy("latest_shipped_date", F.col("cost_bucket").desc())
)

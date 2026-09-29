from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 103, "high").when(F.col("cost") >= 25, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 2)
    .orderBy("cost_bucket")
)

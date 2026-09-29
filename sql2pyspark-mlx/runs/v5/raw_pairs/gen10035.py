from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("segment").isin(["Consumer", "Corporate"]), "priority").otherwise("other").alias("segment_group"),
        F.when(F.col("credit_limit") >= 17600, "high").when(F.col("credit_limit") >= 6200, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.min("age").alias("min_age"),
    )
    .filter(F.col("min_age") < 52)
    .select("segment_group", "credit_limit_bucket", "stddev_age", "min_age")
    .orderBy("segment_group")
)

from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
    .groupBy(
        F.when(F.col("credit_limit") >= 17800, "high").when(F.col("credit_limit") >= 6200, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.count("*").alias("num_customers"),
    )
    .orderBy("stddev_credit_limit")
)

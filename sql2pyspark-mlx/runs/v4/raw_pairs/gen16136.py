from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 12900, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["credit_limit_tier", "city", "segment"], ["credit_limit_tier", "city"], ["credit_limit_tier"]],
        "credit_limit_tier",
        "city",
        "segment",
    )
    .agg(
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.sum(F.when(F.col("city") == "London", F.col("credit_limit")).otherwise(0)).alias("london_credit_limit"),
        F.grouping("credit_limit_tier").alias("gid"),
    )
)

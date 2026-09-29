from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("week_start", F.date_trunc("WEEK", "signup_date"))
    .withColumn("age_tier", F.when(F.col("age") >= 65, "HIGH").otherwise("LOW"))
    .rollup("week_start", "city", "age_tier")
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("num_segments") < 1)
    .orderBy("age_tier")
)

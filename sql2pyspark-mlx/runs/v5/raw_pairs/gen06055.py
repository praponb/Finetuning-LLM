from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 17100, "PREMIUM").otherwise("STANDARD"))
    .withColumn("age_tier", F.when(F.col("age") >= 65, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["credit_limit_tier", "age_tier"], ["credit_limit_tier"]],
        "credit_limit_tier",
        "age_tier",
    )
    .agg(
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.countDistinct("country").alias("distinct_countries"),
        F.bit_and("age").alias("age_bit_and"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("age_tier")
)

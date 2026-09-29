from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNotNull())
    .groupBy(
        F.when(F.col("segment").isin(["Corporate", "Consumer"]), "tier_1").otherwise("other").alias("segment_group"),
        "city",
    )
    .agg(
        F.count("*").alias("num_customers"),
        F.sum(F.when(F.col("country") == "UK", F.col("credit_limit")).otherwise(0)).alias("uk_credit_limit"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .orderBy("latest_signup_date")
)

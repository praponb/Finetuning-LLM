from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("credit_limit").isNotNull() & (F.col("segment") == "SMB"))
    .withColumn("country_lower", F.lower("country"))
)

customers_stats = (
    base
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "city",
    )
    .agg(
        F.count("*").alias("num_customers"),
        F.count("age").alias("age_count"),
    )
)

result = (
    customers_stats
    .select("signup_cohort", "city", "num_customers", "age_count")
    .orderBy("signup_cohort")
)

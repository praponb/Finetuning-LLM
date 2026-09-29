from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("email").isNotNull() & F.col("email").like("%@mail.co.th")) | (F.col("age") != 58))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

summary = (
    src
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.round(((100 * F.sum(F.when(F.col("country") == "UK", 1).otherwise(0))) / F.count("*")), 1).alias("pct_uk"),
    )
)

result = (
    summary
    .filter(F.col("corporate_count") < 8)
    .select("signup_month", "corporate_count")
    .orderBy(F.col("corporate_count").desc(), "signup_month")
    .limit(50)
)

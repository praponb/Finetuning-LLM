from pyspark.sql import functions as F

src = (
    df_customers
    .filter(F.col("country") == "DE")
    .withColumn("adjusted_credit_limit", F.round((F.col("credit_limit") * 100), 2))
)

customers_stats = (
    src
    .groupBy("city")
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.max("signup_date").alias("last_signup_date"),
    )
)

result = (
    customers_stats
    .select("city", "max_credit_limit", "last_signup_date")
    .orderBy(F.col("max_credit_limit").desc())
)

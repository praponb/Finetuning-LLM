from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") == 54)
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("s.plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
    .filter(F.col("standard_count") > 69)
    .orderBy(F.col("signup_month").desc())
)

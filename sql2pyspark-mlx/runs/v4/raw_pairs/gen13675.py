from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 72, "high").when(F.col("age") >= 26, "mid").otherwise("low").alias("age_bucket"),
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
    )
    .agg(
        F.count("*").alias("customer_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

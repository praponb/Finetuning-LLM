from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "is_active")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.min("signup_date").alias("first_signup_date"),
    )
)

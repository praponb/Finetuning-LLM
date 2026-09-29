from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("age").isNotNull() | F.col("name").isNotNull()))
    .rollup(
        "country",
        F.when(F.col("age") >= 72, "high").when(F.col("age") >= 28, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.min("signup_date").alias("first_signup_date"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

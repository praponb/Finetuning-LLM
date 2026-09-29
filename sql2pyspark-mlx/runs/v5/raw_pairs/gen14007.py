from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@mail.co.th"))
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
        F.max("signup_date").alias("latest_signup_date"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

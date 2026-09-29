from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

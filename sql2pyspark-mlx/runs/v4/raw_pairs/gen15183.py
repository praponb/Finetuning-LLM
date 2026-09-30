from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"), "country")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
    )
    .filter(F.col("corr_credit_limit_age") >= 0)
)

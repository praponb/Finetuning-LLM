from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([5, 11, 12]) & F.col("credit_limit").between(5800, 15500))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy("signup_month", "country_values")
)

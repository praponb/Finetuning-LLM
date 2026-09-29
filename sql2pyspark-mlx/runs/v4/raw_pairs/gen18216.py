from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@GMAIL.COM"))
    .groupBy(
        F.when(F.col("country").isin(["JP", "DE"]), "core").otherwise("other").alias("country_group"),
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
    )
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
)

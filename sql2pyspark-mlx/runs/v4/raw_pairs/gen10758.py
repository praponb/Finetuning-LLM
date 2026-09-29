from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") < "2024-11-01") | (F.col("customer_id") == 19))
    .groupBy("status", "plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

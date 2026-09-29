from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 40) | (F.year("end_date") >= 2025))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
    .orderBy("status_values")
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "premium", "standard"]) | F.col("customer_id").isin([3, 20, 26]))
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"), "plan")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .filter(F.col("enterprise_count") < 2)
)

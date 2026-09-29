from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-12-21")
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("cnt"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
)

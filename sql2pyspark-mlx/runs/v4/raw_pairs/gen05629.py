from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") <= "2024-02-01")
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy(F.col("start_month").desc())
    .limit(10)
)

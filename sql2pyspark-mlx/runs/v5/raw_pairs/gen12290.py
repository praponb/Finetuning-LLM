from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("start_date") >= "2023-05-18")
    .withColumn("plan_length", F.length("plan"))
)

agg = (
    filtered_subscriptions
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    agg
    .select("status", "status_list")
    .orderBy("status")
    .limit(20)
)

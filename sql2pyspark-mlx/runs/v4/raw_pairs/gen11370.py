from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("start_date") < "2023-05-01")

plan_summary = (
    filtered_subscriptions
    .groupBy("plan")
    .agg(F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"))
)

result = plan_summary.select("plan", "basic_count")

from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("plan") == "premium")

agg = (
    valid_subscriptions
    .groupBy("customer_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
)

result = (
    agg
    .select("customer_id", "plan_values")
    .orderBy("customer_id", F.col("plan_values").desc())
)

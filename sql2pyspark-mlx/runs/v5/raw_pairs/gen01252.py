from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
)

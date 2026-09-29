from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
)

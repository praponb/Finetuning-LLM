from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(130, 210))
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
    .orderBy("status")
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "standard") & (F.col("monthly_fee") >= 200) & F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
)

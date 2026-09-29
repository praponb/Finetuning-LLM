from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(~F.col("plan").isin(["enterprise", "standard", "basic"]) & F.col("customer_id").isin([31, 35]) & (F.col("plan") != "enterprise"))
    .withColumn("plan_status", F.concat("plan", F.lit(", "), "status"))
)

summary = (
    base
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    summary
    .select("status", "status_list")
    .orderBy("status")
)

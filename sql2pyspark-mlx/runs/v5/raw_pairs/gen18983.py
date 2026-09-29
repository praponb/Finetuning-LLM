from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE", "PAUSED"]) & ~F.col("plan").isin(["premium", "basic", "standard"]))
    .groupBy(
        "status",
        F.when(F.col("plan").isin(["premium", "enterprise"]), "priority").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("*").alias("num_subscriptions"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["basic", "standard", "enterprise"]) & F.col("customer_id").isin([6, 16, 28, 33]) & (F.col("plan") == "basic"))
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
    .filter(F.col("cancelled_count") > 71)
)

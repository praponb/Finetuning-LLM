from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("monthly_fee") < 160)

agg = (
    src
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
)

result = (
    agg
    .filter(F.col("cancelled_monthly_fee") < 34)
    .orderBy("plan_values", "plan")
)

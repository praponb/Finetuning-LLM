from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("created_at", "id", "k")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("attributes").alias("k", "v"))
)

flagged = (
    exploded
    .withColumn("session_start_flag", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_carts = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

matched = (
    sessions_carts
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.created_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("burst_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("burst_id_tier").desc_nulls_last())
)

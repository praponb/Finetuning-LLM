from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("plan", "customer_id")
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy("plan_sum_monthly_fee", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE"]))
    .select(
        "*",
        F.round((F.col("monthly_fee") * 0.85), 2).alias("adjusted_monthly_fee"),
        F.when(F.col("plan").isin(["enterprise", "standard"]), "Group A").otherwise("Other").alias("plan_group"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.lead("monthly_fee", 2).over(w).alias("next_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("plan_sum_monthly_fee"),
        F.sum("adjusted_monthly_fee").over(w3).alias("running_adjusted_monthly_fee"),
    )
)

joined = (
    with_windows
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"))
)

enriched = (
    joined
    .withColumn("prev_plan_sum_monthly_fee", F.lag("plan_sum_monthly_fee", 1).over(w4))
    .withColumn("credit_limit_bucket", F.ntile(3).over(w5))
)

filtered_subscriptions = (
    enriched
    .filter((F.col("start_date") > F.to_date(F.lit("2023-11-05"))) & F.col("customer_id").isNotNull())
    .withColumn("credit_limit_rounded", F.round("credit_limit", 2))
    .withColumn("prev_plan_sum_monthly_fee_per_adjusted_monthly_fee", (F.col("prev_plan_sum_monthly_fee") / F.nullif("adjusted_monthly_fee", F.lit(0))))
)

grouped = (
    filtered_subscriptions
    .groupBy(
        "status",
        F.when(F.col("credit_limit_rounded") >= 76, "PREMIUM").otherwise("STANDARD").alias("credit_limit_rounded_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
)

result = (
    grouped
    .select("status", "credit_limit_rounded_tier", "num_plans", "basic_rows")
    .orderBy("num_plans")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first())
w3 = Window.partitionBy("status").orderBy(F.col("end_date_weekofyear").desc(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic"]))
    .withColumn("end_date_weekofyear", F.weekofyear("end_date"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.percent_rank().over(w2).alias("monthly_fee_percent_rank"),
        F.ntile(5).over(w3).alias("end_date_weekofyear_bucket"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.signup_date"))
)

matched_keys = (
    joined
    .select("id", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.year("end_date") >= 2025)
        .select("id", "plan")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube("plan")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.max(F.when(F.col("status") == "PAUSED", F.col("monthly_fee"))).alias("paused_max_monthly_fee"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
        F.grouping("plan").alias("grouping_level"),
    )
    .orderBy(F.col("paused_max_monthly_fee").desc_nulls_first())
)

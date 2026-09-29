from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("rn", "id")

valid = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-10-21").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.concat("status", F.lit("/"), "plan").alias("status_plan"))
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 259, "PREMIUM").otherwise("STANDARD"))
    .cube("monthly_fee_tier", "status")
    .agg(
        F.max(F.when(F.col("plan") == "enterprise", F.col("monthly_fee"))).alias("enterprise_max_monthly_fee"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("monthly_fee_stddev") >= 0)
    .orderBy(F.col("enterprise_max_monthly_fee").desc())
)

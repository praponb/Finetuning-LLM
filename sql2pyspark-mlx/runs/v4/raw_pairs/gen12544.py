from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

ranked = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 269, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["monthly_fee_tier", "customer_id"], ["monthly_fee_tier"]],
        "monthly_fee_tier",
        "customer_id",
    )
    .agg(
        F.avg(F.when(F.col("plan") == "basic", F.col("monthly_fee"))).alias("basic_avg_monthly_fee"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.grouping_id().alias("grouping_id"),
    )
)

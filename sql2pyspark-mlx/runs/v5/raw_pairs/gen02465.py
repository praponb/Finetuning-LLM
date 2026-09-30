from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan", "status").orderBy(F.col("prev_monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy("next_monthly_fee", "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("status", "plan").orderBy(F.col("row_seq").asc_nulls_last(), "id")

enriched = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        F.lead("monthly_fee").over(w_ordered).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w_ordered).alias("prev_monthly_fee"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

metrics = (
    enriched
    .filter(~F.col("plan").isin(["standard", "enterprise"]))
    .select(
        "id",
        "plan",
        "status",
        "next_monthly_fee",
        "row_seq",
        F.row_number().over(w2).alias("event_seq"),
        F.lead("monthly_fee", 1).over(w3).alias("next_monthly_fee_2"),
        F.min("next_monthly_fee").over(w4).alias("status_min_next_monthly_fee"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid = (
    latest
    .filter(F.col("status_min_next_monthly_fee") < 225)
    .select(
        "*",
        F.trim("plan").alias("clean_plan"),
        F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("row_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
        "plan",
    )
    .agg(
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .orderBy(F.col("row_seq_tier").desc())
)

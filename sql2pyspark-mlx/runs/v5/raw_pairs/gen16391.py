from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("start_date").asc_nulls_first(), "id")
w_ordered = Window.partitionBy("status").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("plan", "status")
w4 = Window.partitionBy("status").orderBy(F.col("next_monthly_fee").desc_nulls_last())
w5 = Window.orderBy("row_seq", "id")
w6 = Window.partitionBy("status").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    ranked
    .filter((F.col("end_date") >= F.lit("2023-07-24").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("plan").isin(["standard", "basic"]), "Group A").otherwise("Other").alias("plan_group"),
        F.date_format("end_date", "yyyy-MM-dd").alias("end_date_label"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "status",
        "dedup_rank",
        "plan_group",
        "end_date_label",
        F.lead("monthly_fee").over(w_ordered).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w_ordered).alias("prev_monthly_fee"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4).alias("monthly_fee_share"),
    )
)

enriched = (
    metrics
    .filter(F.col("monthly_fee_share").isNull())
    .select(
        "id",
        "status",
        "plan_group",
        "row_seq",
        F.rank().over(w4).alias("next_monthly_fee_rank"),
        F.row_number().over(w5).alias("event_seq"),
        F.first("row_seq", ignorenulls=True).over(w6).alias("first_known_row_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("first_known_row_seq") >= 9, "HIGH").otherwise("LOW").alias("first_known_row_seq_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)

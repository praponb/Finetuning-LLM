from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")
w_part_2 = Window.partitionBy("customer_id", "status").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("plan").orderBy("row_seq", "id")
w4 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee_bucket_str", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.partitionBy("customer_id", "plan").orderBy("prev_monthly_fee_bucket", "id")
w6 = Window.partitionBy("customer_id")

with_windows = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        F.row_number().over(w_part).alias("row_seq"),
        F.ntile(4).over(w_part_2).alias("monthly_fee_bucket"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.col("monthly_fee_bucket").cast("double").alias("monthly_fee_bucket_str"),
    )
)

with_windows_subscriptions = (
    cleaned
    .select(
        "id",
        "customer_id",
        "plan",
        "row_seq",
        "monthly_fee_bucket_str",
        F.lag("row_seq").over(w3).alias("prev_row_seq"),
        F.lead("row_seq").over(w3).alias("next_row_seq"),
        F.lead("monthly_fee_bucket").over(w3).alias("next_monthly_fee_bucket"),
        F.lag("monthly_fee_bucket").over(w3).alias("prev_monthly_fee_bucket"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("monthly_fee_bucket_str").over(w4).alias("monthly_fee_bucket_str_avg_5"),
    )
)

enriched = (
    with_windows_subscriptions
    .select(
        "*",
        F.lead("next_row_seq").over(w_running).alias("next_next_row_seq"),
        F.lag("next_row_seq").over(w_running).alias("prev_next_row_seq"),
        F.lead("row_seq").over(w_running).alias("next_row_seq_2"),
        F.lag("row_seq").over(w_running).alias("prev_row_seq_2"),
        F.row_number().over(w_running).alias("row_seq_2"),
        F.round((F.col("prev_row_seq") / F.sum("prev_row_seq").over(w6)), 4).alias("prev_row_seq_share"),
    )
)

base = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round("prev_next_row_seq", 0).alias("prev_next_row_seq_rounded"),
        F.round("next_next_row_seq", 1).alias("next_next_row_seq_rounded"),
    )
)

result = (
    base
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["country", "segment", "plan"], ["country"]],
        F.col("c.country").alias("country"),
        F.col("c.segment").alias("segment"),
        F.col("e.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.prev_next_row_seq_rounded")).alias("total_prev_next_row_seq_rounded"),
        F.min(F.col("e.row_seq")).alias("min_row_seq"),
    )
)

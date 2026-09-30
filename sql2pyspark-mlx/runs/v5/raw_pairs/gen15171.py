from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("last_restock_plus_57d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("last_restock_plus_57d", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("last_restock", "id").rowsBetween(-5, -1)
w4 = Window.orderBy("days_last_restock_plus_57d_to_last_restock_avg_6", "id")
w5 = Window.orderBy("last_restock_weekofyear_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("cumulative_last_restock_weekofyear")
w_part = Window.orderBy("running_last_restock_weekofyear_2", "id")
w8 = Window.orderBy("row_seq", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.orderBy("next_last_restock_weekofyear_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-08-29")))
    .select(
        "id",
        "last_restock",
        F.date_add("last_restock", 57).alias("last_restock_plus_57d"),
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
    )
)

cleaned = (
    filtered
    .filter(F.col("last_restock_weekofyear").between(43, 76))
    .withColumn("days_last_restock_plus_57d_to_last_restock", F.datediff("last_restock", "last_restock_plus_57d"))
)

enriched = (
    cleaned
    .filter(F.year("last_restock_plus_57d") >= 2025)
    .withColumn("running_last_restock_weekofyear", F.sum("last_restock_weekofyear").over(w_running))
    .withColumn("days_last_restock_plus_57d_to_last_restock_avg_6", F.avg("days_last_restock_plus_57d_to_last_restock").over(w2))
    .withColumn("last_restock_weekofyear_avg_5_prior", F.avg("last_restock_weekofyear").over(w3))
    .withColumn("cumulative_last_restock_weekofyear", F.sum("last_restock_weekofyear").over(w_running))
)

with_windows = (
    enriched
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("next_last_restock_weekofyear_avg_5_prior", F.lead("last_restock_weekofyear_avg_5_prior", 1).over(w4))
    .withColumn("running_last_restock_weekofyear_2", F.sum("last_restock_weekofyear").over(w5))
    .withColumn("cumulative_last_restock_weekofyear_cume_dist", F.cume_dist().over(w6))
)

enriched_inventory = (
    with_windows
    .withColumn("prev_running_last_restock_weekofyear_2", F.lag("running_last_restock_weekofyear_2").over(w_part))
    .withColumn("prev_last_restock_weekofyear", F.lag("last_restock_weekofyear").over(w_part))
    .withColumn("row_seq_2", F.row_number().over(w_part))
)

enriched_inventory_2 = (
    enriched_inventory
    .select(
        "id",
        "last_restock_plus_57d",
        "days_last_restock_plus_57d_to_last_restock_avg_6",
        "last_restock_weekofyear_avg_5_prior",
        "cumulative_last_restock_weekofyear",
        "prev_running_last_restock_weekofyear_2",
        "prev_last_restock_weekofyear",
        F.avg("row_seq").over(w8).alias("last4_avg_row_seq"),
        F.count("row_seq").over(w8).alias("last4_count_row_seq"),
        F.max("row_seq").over(w8).alias("last4_max_row_seq"),
        F.min("row_seq").over(w8).alias("last4_min_row_seq"),
        F.last("cumulative_last_restock_weekofyear_cume_dist", ignorenulls=True).over(w9).alias("last_known_cumulative_last_restock_weekofyear_cume_dist"),
    )
)

grouped = (
    enriched_inventory_2
    .withColumn("activity_month", F.date_trunc("month", "last_restock_plus_57d"))
    .withColumn("days_last_restock_plus_57d_to_last_restock_avg_6_tier", F.when(F.col("days_last_restock_plus_57d_to_last_restock_avg_6") >= 70, "PREMIUM").otherwise("STANDARD"))
    .cube("activity_month", "days_last_restock_plus_57d_to_last_restock_avg_6_tier")
    .agg(
        F.percentile_approx("days_last_restock_plus_57d_to_last_restock_avg_6", 0.95).alias("days_last_restock_plus_57d_to_last_restock_avg_6_p95"),
        F.round(F.avg("last4_avg_row_seq"), 2).alias("avg_last4_avg_row_seq"),
        F.percentile_approx("prev_last_restock_weekofyear", 0.95).alias("prev_last_restock_weekofyear_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    grouped
    .select(
        "days_last_restock_plus_57d_to_last_restock_avg_6_p95",
        "avg_last4_avg_row_seq",
        "prev_last_restock_weekofyear_p95",
        "distinct_ids",
    )
    .orderBy(F.col("avg_last4_avg_row_seq").desc_nulls_first())
)

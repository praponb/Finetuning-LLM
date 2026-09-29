from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")
w3 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("units_bucket", "id")
w5 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("units_bucket", "id").rowsBetween(-3, -1)
w7 = Window.orderBy("units_avg_3_prior_times_row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("least_row_seq_2_units_bucket", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy(F.col("running_row_seq_per_units_bucket").asc_nulls_first())
w_ordered = Window.orderBy("row_seq_2", "id").rowsBetween(-6, Window.currentRow)
w11 = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-03-15").cast("date")) & (F.col("units") != 20))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "sale_date",
        "units",
        F.ntile(4).over(w).alias("units_bucket"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

metrics = (
    enriched
    .withColumn("running_row_seq", F.sum("row_seq").over(w3))
    .withColumn("row_seq_2", F.row_number().over(w_recent))
    .withColumn("cumulative_row_seq", F.sum("row_seq").over(w5))
    .withColumn("units_avg_3_prior", F.avg("units").over(w6))
)

cleaned = (
    metrics
    .filter(F.col("sale_date") >= F.lit("2023-06-12").cast("date"))
    .withColumn("units_avg_3_prior_filled", F.coalesce("units_avg_3_prior", F.lit(0)))
    .withColumn("running_row_seq_per_units_bucket", F.round((F.col("running_row_seq") / F.col("units_bucket")), 2))
)

filtered = (
    cleaned
    .filter(F.col("row_seq_2") > 5)
    .select(
        "id",
        "units",
        "row_seq",
        "running_row_seq",
        "row_seq_2",
        "cumulative_row_seq",
        "units_avg_3_prior_filled",
        "running_row_seq_per_units_bucket",
        (F.col("units_avg_3_prior") * F.col("row_seq_2")).alias("units_avg_3_prior_times_row_seq_2"),
        F.least("row_seq_2", "units_bucket").alias("least_row_seq_2_units_bucket"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "row_seq",
        "running_row_seq",
        "row_seq_2",
        "running_row_seq_per_units_bucket",
        F.first("running_row_seq", ignorenulls=True).over(w7).alias("first_known_running_row_seq"),
        F.avg("running_row_seq_per_units_bucket").over(w8).alias("running_row_seq_per_units_bucket_avg_6"),
        F.percent_rank().over(w9).alias("running_row_seq_per_units_bucket_percent_rank"),
    )
)

enriched_store_sales = (
    scored
    .filter(F.col("row_seq_2") != 3)
    .select(
        "*",
        F.avg("row_seq_2").over(w_ordered).alias("row_seq_2_avg_6"),
        F.first("running_row_seq", ignorenulls=True).over(w11).alias("first_known_running_row_seq_2"),
    )
)

result = (
    enriched_store_sales
    .groupBy(
        F.when(F.col("row_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.percentile_approx("first_known_running_row_seq_2", [0.25, 0.5, 0.75]).alias("first_known_running_row_seq_2_quartiles"),
        F.round(F.avg("row_seq_2"), 2).alias("avg_row_seq_2"),
        F.count("*").alias("event_count"),
        F.percentile_approx("row_seq_2", [0.25, 0.5, 0.75]).alias("row_seq_2_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("row_seq_tier").asc_nulls_first())
)

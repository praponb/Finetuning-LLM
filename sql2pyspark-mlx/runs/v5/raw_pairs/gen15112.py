from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w_recent = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy("store_id").orderBy("sale_date", "id")
w4 = Window.orderBy("row_seq_times_last7_sum_units", "id")
w5 = Window.orderBy("last7_sum_units", "id").rowsBetween(-2, -1)
w6 = Window.orderBy(F.col("next_last7_max_units").desc(), "id")
w7 = Window.orderBy("prev_last7_max_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-24").cast("date")) & F.col("sale_date").between("2023-06-01", "2023-07-01"))
    .withColumn("units_tier", F.when(F.col("units") >= 34, "High").when(F.col("units") >= 8, "Medium").otherwise("Low"))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    deduped
    .filter(F.col("store_id").isin([3, 4]))
    .select(
        "id",
        "units_tier",
        F.sum("units").over(w_recent).alias("last7_sum_units"),
        F.max("units").over(w_recent).alias("last7_max_units"),
        F.avg("units").over(w_recent).alias("last7_avg_units"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

cleaned_store_sales = (
    metrics
    .filter(F.col("last7_max_units").between(15, 39))
    .select(
        "*",
        (F.col("row_seq") * F.col("last7_sum_units")).alias("row_seq_times_last7_sum_units"),
        F.when(F.col("last7_max_units") >= 36, "High").when(F.col("last7_max_units") >= 20, "Medium").otherwise("Low").alias("last7_max_units_level"),
    )
)

metrics_store_sales = (
    cleaned_store_sales
    .select(
        "*",
        F.lag("last7_max_units").over(w4).alias("prev_last7_max_units"),
        F.lead("last7_max_units").over(w4).alias("next_last7_max_units"),
        F.lag("row_seq").over(w4).alias("prev_row_seq"),
        F.lead("row_seq").over(w4).alias("next_row_seq"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("last7_sum_units").over(w5).alias("last7_sum_units_avg_2_prior"),
    )
)

scored = (
    metrics_store_sales
    .select(
        "id",
        "units_tier",
        "row_seq",
        "next_last7_max_units",
        "next_row_seq",
        "last7_sum_units_avg_2_prior",
        F.row_number().over(w6).alias("row_seq_2"),
        F.sum("row_seq_times_last7_sum_units").over(w7).alias("cumulative_row_seq_times_last7_sum_units"),
    )
)

summary = (
    scored
    .groupBy(
        F.when(F.col("next_last7_max_units") >= 44, "LARGE").otherwise("SMALL").alias("next_last7_max_units_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last7_sum_units_avg_2_prior", 0.5).alias("last7_sum_units_avg_2_prior_p50"),
        F.percentile_approx("last7_sum_units_avg_2_prior", 0.95).alias("last7_sum_units_avg_2_prior_p95"),
    )
    .filter(F.col("last7_sum_units_avg_2_prior_p50") >= 0)
)

result = (
    summary
    .select("next_last7_max_units_tier")
    .orderBy("next_last7_max_units_tier")
)

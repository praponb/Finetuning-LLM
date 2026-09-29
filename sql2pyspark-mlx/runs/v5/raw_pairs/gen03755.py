from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")
w_grp = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.orderBy(F.col("cost").desc(), "id")
w4 = Window.orderBy(F.col("cost").desc_nulls_last())
w_part = Window.orderBy("row_seq", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy(F.col("last7_min_row_seq").desc())
w7 = Window.orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("status") == "DELIVERED"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        F.substring("status", 1, 2).alias("status_prefix"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "shipped_date", "cost", "carrier_title", "resolved_carrier")
)

scored = (
    latest
    .select(
        "id",
        "shipped_date",
        "cost",
        "resolved_carrier",
        F.sum("cost").over(w_grp).alias("running_cost"),
        F.row_number().over(w_grp_2).alias("row_seq"),
        F.cume_dist().over(w4).alias("cost_cume_dist"),
    )
)

base = (
    scored
    .filter(F.col("shipped_date") >= F.lit("2023-08-30").cast("date"))
    .select(
        "id",
        "shipped_date",
        "cost",
        "resolved_carrier",
        "running_cost",
        "row_seq",
        "cost_cume_dist",
        F.col("cost_cume_dist").cast("int").alias("cost_cume_dist_int"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "resolved_carrier",
        "running_cost",
        "shipped_date_period",
        F.min("row_seq").over(w_part).alias("last7_min_row_seq"),
        F.avg("row_seq").over(w_part).alias("last7_avg_row_seq"),
        F.sum("row_seq").over(w_part).alias("last7_sum_row_seq"),
    )
)

scored_shipments = (
    with_windows
    .select(
        "id",
        "running_cost",
        "last7_min_row_seq",
        "last7_sum_row_seq",
        F.rank().over(w6).alias("last7_min_row_seq_rank"),
        F.count("last7_avg_row_seq").over(w7).alias("last7_avg_row_seq_count_90d"),
    )
)

daily = (
    scored_shipments
    .rollup(
        F.when(F.col("last7_min_row_seq_rank") >= 9, "LARGE").otherwise("SMALL").alias("last7_min_row_seq_rank_tier"),
        F.when(F.col("last7_sum_row_seq") >= 34, "PREMIUM").otherwise("STANDARD").alias("last7_sum_row_seq_tier"),
    )
    .agg(
        F.percentile_approx("last7_avg_row_seq_count_90d", [0.25, 0.5, 0.75]).alias("last7_avg_row_seq_count_90d_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.min("last7_avg_row_seq_count_90d").alias("min_last7_avg_row_seq_count_90d"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    daily
    .select(
        "last7_sum_row_seq_tier",
        "last7_avg_row_seq_count_90d_quartiles",
        "cnt",
        "min_last7_avg_row_seq_count_90d",
        "distinct_ids_2",
        "num_rows",
    )
)

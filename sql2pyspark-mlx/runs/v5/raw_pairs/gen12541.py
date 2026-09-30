from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("product_id").orderBy("revenue_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("revenue")
w7 = Window.orderBy("units_avg_3_prior", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("row_seq")
w_grp_2 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-2, -1)
w10 = Window.partitionBy("product_id").orderBy(F.col("last_known_units").desc_nulls_last())
w_hist = Window.partitionBy("product_id").orderBy("running_row_seq", "id").rowsBetween(-6, -1)
w12 = Window.partitionBy("product_id").orderBy("product_id_avg_revenue_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("product_id").orderBy("product_id_avg_revenue_avg_2_prior", "id")

enriched = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 6]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.row_number().over(w).alias("row_seq"),
        F.last("units", ignorenulls=True).over(w2).alias("last_known_units"),
        F.avg("units").over(w_grp).alias("units_avg_3_prior"),
        F.avg("revenue").over(w_grp).alias("revenue_avg_3_prior"),
        F.avg("revenue").over(w4).alias("product_id_avg_revenue"),
    )
)

scored = (
    enriched
    .withColumn("running_row_seq", F.sum("row_seq").over(w5))
    .withColumn("revenue_percent_rank", F.percent_rank().over(w6))
    .withColumn("units_avg_3_prior_avg_5", F.avg("units_avg_3_prior").over(w7))
    .withColumn("row_seq_rank", F.rank().over(w8))
    .withColumn("product_id_avg_revenue_avg_2_prior", F.avg("product_id_avg_revenue").over(w_grp_2))
)

with_windows = (
    scored
    .select(
        "id",
        "product_id",
        "row_seq",
        "units_avg_3_prior",
        "revenue_avg_3_prior",
        "product_id_avg_revenue",
        "running_row_seq",
        "product_id_avg_revenue_avg_2_prior",
        F.dense_rank().over(w10).alias("last_known_units_dense_rank"),
        F.avg("row_seq").over(w_hist).alias("row_seq_avg_6_prior"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "id",
        "product_id_avg_revenue_avg_2_prior",
        "last_known_units_dense_rank",
        "row_seq_avg_6_prior",
        F.last("revenue_avg_3_prior", ignorenulls=True).over(w12).alias("last_known_revenue_avg_3_prior"),
        F.row_number().over(w13).alias("seq_no"),
        F.count("units_avg_3_prior").over(w4).alias("product_id_count_units_avg_3_prior"),
    )
)

result = (
    with_windows_store_sales
    .groupBy(
        F.when(F.col("last_known_units_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("last_known_units_dense_rank_tier"),
        F.when(F.col("last_known_revenue_avg_3_prior") >= 3647, "HIGH").otherwise("LOW").alias("last_known_revenue_avg_3_prior_tier"),
    )
    .agg(
        F.round(F.avg("product_id_count_units_avg_3_prior"), 2).alias("avg_product_id_count_units_avg_3_prior"),
        F.percentile_approx("product_id_count_units_avg_3_prior", 0.95).alias("product_id_count_units_avg_3_prior_p95"),
        F.count("*").alias("event_count"),
        F.percentile_approx("last_known_units_dense_rank", [0.25, 0.5, 0.75]).alias("last_known_units_dense_rank_quartiles"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.percentile_approx("last_known_revenue_avg_3_prior", [0.25, 0.5, 0.75]).alias("last_known_revenue_avg_3_prior_quartiles"),
    )
    .filter(F.col("event_count") >= 1)
    .orderBy(F.col("avg_seq_no").desc())
)

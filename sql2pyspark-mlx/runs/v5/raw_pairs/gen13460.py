from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w_ordered = Window.partitionBy("product_id").orderBy("event_seq", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("prev_event_seq_per_units").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("prev_event_seq_per_units", "id")
w6 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("prev_event_seq_per_units", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("product_id").orderBy("running_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy(F.col("prev_event_seq_per_units").desc())
w10 = Window.partitionBy("product_id").orderBy(F.col("stock_qty_avg_5_prior").desc_nulls_last())

with_windows = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5]))
    .withColumn("units_avg_3", F.avg("units").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

metrics = (
    with_windows
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "units_avg_3",
        F.lag("event_seq").over(w_ordered).alias("prev_event_seq"),
        F.lag("units").over(w_ordered).alias("prev_units"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

cleaned = (
    metrics
    .filter(F.col("seq_no") != 8)
    .select(
        "*",
        (F.col("prev_event_seq") - F.col("seq_no")).alias("prev_event_seq_minus_seq_no"),
        F.round((F.col("prev_event_seq") / F.col("units")), 2).alias("prev_event_seq_per_units"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.units",
        "e.prev_event_seq_minus_seq_no",
        "e.prev_event_seq_per_units",
        F.col("p.stock_qty"),
        F.col("p.product_name"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

with_windows_store_sales = (
    deduped
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("running_units", F.sum("units").over(w6))
    .withColumn("stock_qty_avg_5_prior", F.avg("stock_qty").over(w7))
)

with_windows_store_sales_2 = (
    with_windows_store_sales
    .select(
        "id",
        "product_id",
        "prev_event_seq_minus_seq_no",
        "prev_event_seq_per_units",
        "stock_qty",
        "product_name",
        "rn",
        "row_seq",
        "running_units",
        F.sum("row_seq").over(w8).alias("running_row_seq"),
        F.cume_dist().over(w9).alias("prev_event_seq_per_units_cume_dist"),
        F.rank().over(w10).alias("stock_qty_avg_5_prior_rank"),
    )
)

result = (
    with_windows_store_sales_2
    .groupBy(
        F.when(F.col("prev_event_seq_per_units") >= 79, "HIGH").otherwise("LOW").alias("prev_event_seq_per_units_tier"),
    )
    .agg(
        F.round(F.avg("stock_qty_avg_5_prior_rank"), 2).alias("avg_stock_qty_avg_5_prior_rank"),
        F.min("row_seq").alias("min_row_seq"),
        F.percentile_approx("prev_event_seq_minus_seq_no", [0.25, 0.5, 0.75]).alias("prev_event_seq_minus_seq_no_quartiles"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)

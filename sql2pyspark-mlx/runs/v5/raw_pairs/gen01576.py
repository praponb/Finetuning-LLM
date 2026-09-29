from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w3 = Window.partitionBy("store_id", "product_id")
w4 = Window.partitionBy("product_id").orderBy("units", "id")
w5 = Window.partitionBy("product_id").orderBy("units_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last(), "id")
w7 = Window.orderBy(F.col("next_units").desc_nulls_last())
w8 = Window.partitionBy("product_id").orderBy("prev_units", "id").rowsBetween(-3, Window.currentRow)

metrics = (
    df_store_sales
    .filter(F.col("revenue").between(1600, 2500))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.ntile(4).over(w).alias("revenue_bucket"),
        F.rank().over(w2).alias("units_rank"),
        F.stddev_samp("revenue").over(w3).alias("store_id_stddev_samp_revenue"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lag("units").over(w4).alias("prev_units"),
        F.lead("units").over(w4).alias("next_units"),
        F.lead("revenue").over(w4).alias("next_revenue"),
        F.row_number().over(w4).alias("seq_no"),
        F.last("store_id_stddev_samp_revenue", ignorenulls=True).over(w5).alias("last_known_store_id_stddev_samp_revenue"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "prev_units",
        "last_known_store_id_stddev_samp_revenue",
        F.rank().over(w7).alias("next_units_rank"),
        F.avg("revenue").over(w8).alias("revenue_avg_3"),
    )
)

result = (
    with_windows
    .groupBy("store_id")
    .agg(
        F.round(F.avg("revenue_avg_3"), 2).alias("avg_revenue_avg_3"),
        F.percentile_approx("next_units_rank", 0.95).alias("next_units_rank_p95"),
        F.sum("revenue_avg_3").alias("total_revenue_avg_3"),
    )
    .filter(F.col("next_units_rank_p95") > 0)
)

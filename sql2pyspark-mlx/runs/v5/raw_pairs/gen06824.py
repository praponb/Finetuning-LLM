from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_filled", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last3_count_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("last3_count_revenue", "id")
w_grp = Window.partitionBy("store_id").orderBy(F.col("revenue_filled").desc())
w5 = Window.partitionBy("store_id")
w6 = Window.orderBy(F.col("revenue_filled_rank").desc(), "id")
w7 = Window.partitionBy("store_id").orderBy("seq_no", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy(F.col("last3_count_revenue").desc(), "id")
w9 = Window.orderBy("seq_no", "id")
w10 = Window.partitionBy("store_id").orderBy(F.col("revenue_filled_rank").desc_nulls_first(), "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-05-27").cast("date")) & (F.col("revenue") < 3100))
    .select(
        "id",
        "store_id",
        "revenue",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

metrics = (
    cleaned
    .withColumn("last3_count_revenue", F.count("revenue").over(w))
    .withColumn("last3_avg_revenue", F.avg("revenue").over(w))
    .withColumn("last3_min_revenue", F.min("revenue").over(w))
)

metrics_store_sales = (
    metrics
    .filter(F.col("last3_avg_revenue") <= 3700)
    .select(
        "id",
        "store_id",
        "revenue",
        "last3_count_revenue",
        "last3_min_revenue",
        F.sum("revenue_filled").over(w2).alias("running_revenue_filled"),
        F.row_number().over(w3).alias("seq_no"),
        F.rank().over(w_grp).alias("revenue_filled_rank"),
        F.min("last3_count_revenue").over(w5).alias("store_id_min_last3_count_revenue"),
    )
)

with_windows = (
    metrics_store_sales
    .withColumn("revenue_filled_rank_bucket", F.ntile(5).over(w6))
    .withColumn("last3_count_revenue_avg_5", F.avg("last3_count_revenue").over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("seq_no_2", F.row_number().over(w9))
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "store_id",
        "revenue",
        "last3_count_revenue",
        "last3_min_revenue",
        "revenue_filled_rank",
        "revenue_filled_rank_bucket",
        "last3_count_revenue_avg_5",
        "seq_no_2",
    )
    .orderBy(F.col("last3_count_revenue_avg_5").desc(), "last3_count_revenue")
)

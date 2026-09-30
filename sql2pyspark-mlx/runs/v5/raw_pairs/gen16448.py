from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").desc(), "id")
w_running = Window.partitionBy("store_id").orderBy("least_units_revenue", "id").rowsBetween(-5, -1)
w_grp = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w4 = Window.partitionBy("store_id").orderBy("least_units_revenue", "id").rowsBetween(-3, Window.currentRow)
w_grp_2 = Window.partitionBy("store_id").orderBy("revenue", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("revenue_rank").desc_nulls_last(), "id")
w7 = Window.partitionBy("product_id").orderBy("revenue_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy(F.col("least_units_revenue").desc(), "id")
w9 = Window.partitionBy("product_id").orderBy(F.col("revenue_avg_3").desc_nulls_last())
w10 = Window.partitionBy("product_id").orderBy(F.col("least_units_revenue").desc())

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-09-13")))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)

with_windows = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("units_avg_5_prior", F.avg("units").over(w_running))
    .withColumn("revenue_rank", F.rank().over(w_grp))
    .withColumn("revenue_avg_3", F.avg("revenue").over(w4))
    .withColumn("revenue_bucket", F.ntile(5).over(w_grp_2))
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "least_units_revenue",
        "units_avg_5_prior",
        "revenue_rank",
        "revenue_bucket",
        F.sum("least_units_revenue").over(w7).alias("running_least_units_revenue"),
        F.ntile(5).over(w8).alias("least_units_revenue_bucket"),
        F.dense_rank().over(w9).alias("revenue_avg_3_dense_rank"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w10))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "store_id",
        "least_units_revenue",
        "units_avg_5_prior",
        "running_least_units_revenue",
        "least_units_revenue_bucket",
        "revenue_avg_3_dense_rank",
    )
)

result = (
    latest
    .groupBy(
        "store_id",
        F.when(F.col("running_least_units_revenue") >= 385, "LARGE").otherwise("SMALL").alias("running_least_units_revenue_tier"),
        F.when(F.col("least_units_revenue_bucket") >= 9, "HIGH").otherwise("LOW").alias("least_units_revenue_bucket_tier"),
    )
    .agg(
        F.percentile_approx("units_avg_5_prior", 0.5).alias("units_avg_5_prior_p50"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("cnt").desc())
)

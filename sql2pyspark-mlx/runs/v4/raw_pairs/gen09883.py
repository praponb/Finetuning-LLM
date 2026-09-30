from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("revenue", "id").rowsBetween(-5, -1)
w_ordered = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("sale_date", "id")
w4 = Window.partitionBy("city").orderBy(F.col("units").desc_nulls_last(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("units_rounded_avg_5_prior").desc_nulls_first())
w6 = Window.orderBy(F.col("prev_units_rounded").desc_nulls_last())
w7 = Window.partitionBy("store_id").orderBy(F.col("units_rounded").desc_nulls_last())
w8 = Window.partitionBy("store_id", "product_id")
w9 = Window.orderBy(F.col("units_rounded_dense_rank").asc_nulls_first())
w10 = Window.partitionBy("product_id").orderBy(F.col("store_id_min_last_known_revenue").desc())
w11 = Window.partitionBy("product_id").orderBy("units")
w12 = Window.partitionBy("city")

enriched = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.city"))
)

cleaned = (
    enriched
    .filter((F.col("sale_date") >= F.lit("2023-06-14").cast("date")) & F.col("store_id").isin([1, 4, 5]))
    .withColumn("units_rounded", F.round("units", 2))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

with_windows = (
    cleaned
    .withColumn("units_rounded_avg_5_prior", F.avg("units_rounded").over(w))
    .withColumn("last_known_revenue", F.last("revenue", ignorenulls=True).over(w_ordered))
    .withColumn("prev_units_rounded", F.lag("units_rounded", 1).over(w_recent))
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    ranked
    .select("product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("sale_date") < "2024-06-01")
        .select("product_id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched_store_sales = (
    verified
    .select(
        "*",
        F.dense_rank().over(w5).alias("units_rounded_avg_5_prior_dense_rank"),
        F.rank().over(w6).alias("prev_units_rounded_rank"),
        F.dense_rank().over(w7).alias("units_rounded_dense_rank"),
        F.min("last_known_revenue").over(w8).alias("store_id_min_last_known_revenue"),
    )
)

metrics = (
    enriched_store_sales
    .withColumn("units_rounded_dense_rank_dense_rank", F.dense_rank().over(w9))
    .withColumn("store_id_min_last_known_revenue_cume_dist", F.cume_dist().over(w10))
    .withColumn("units_percent_rank", F.percent_rank().over(w11))
    .withColumn("prev_units_rounded_rank_share", F.round((F.col("prev_units_rounded_rank") / F.sum("prev_units_rounded_rank").over(w12)), 4))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("units_rounded_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("units_rounded_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.sum(F.when(F.col("city") == "London", F.col("store_id_min_last_known_revenue_cume_dist"))).alias("london_total_store_id_min_last_known_revenue_cume_dist"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
    )
    .orderBy(F.col("singapore_rows").desc_nulls_last())
)

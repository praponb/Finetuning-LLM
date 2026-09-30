from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("sqft")
w3 = Window.orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region").orderBy("sqft_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("sqft_dense_rank").desc())
w6 = Window.partitionBy("region").orderBy(F.col("sqft_bucket").desc(), "id")
w7 = Window.partitionBy("city").orderBy(F.col("cumulative_sqft_bucket").desc_nulls_last())
w8 = Window.partitionBy("city").orderBy(F.col("sqft_bucket").desc())
w9 = Window.partitionBy("region")

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-11-14"))) & F.col("region").isNotNull())
    .select("*", F.trim("store_name").alias("clean_store_name"))
)

metrics = (
    cleaned
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w2))
    .withColumn("sqft_bucket", F.ntile(4).over(w3))
)

metrics_stores = (
    metrics
    .select(
        "*",
        F.sum("sqft_bucket").over(w4).alias("cumulative_sqft_bucket"),
        F.cume_dist().over(w5).alias("sqft_dense_rank_cume_dist"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

kept_keys = (
    metrics_stores
    .select("city", "region")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["Central", "South", "North"]))
        .select("city", "region")
    )
)

reconciled = (
    metrics_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") <= F.col("s.opened_date")), "left_semi")
)

scored = (
    kept
    .select(
        "*",
        F.dense_rank().over(w7).alias("cumulative_sqft_bucket_dense_rank"),
        F.dense_rank().over(w8).alias("sqft_bucket_dense_rank"),
        F.sum("sqft").over(w9).alias("region_sum_sqft"),
    )
)

cleaned_stores = (
    scored
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-12-30"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.when(F.col("region_sum_sqft") >= 85904, "High").when(F.col("region_sum_sqft") >= 14042, "Medium").otherwise("Low").alias("region_sum_sqft_level"),
        (F.col("sqft_dense_rank_cume_dist") * F.col("region_sum_sqft")).alias("sqft_dense_rank_cume_dist_times_region_sum_sqft"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

result = (
    cleaned_stores
    .select(
        "city",
        "sqft_dense_rank_cume_dist",
        "sqft_dense_rank_cume_dist_times_region_sum_sqft",
    )
    .unpivot(
        ["city"],
        ["sqft_dense_rank_cume_dist", "sqft_dense_rank_cume_dist_times_region_sum_sqft"],
        "metric_name",
        "val",
    )
    .filter(F.col("val").isNotNull())
)

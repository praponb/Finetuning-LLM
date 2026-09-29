from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w4 = Window.partitionBy("region").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("sqft", "id")
w6 = Window.partitionBy("region").orderBy("sqft_bucket", "id")
w7 = Window.partitionBy("city", "region").orderBy("dedup_rank", "id")
w8 = Window.orderBy("dedup_rank_2", "id")
w9 = Window.partitionBy("region", "city").orderBy(F.col("last_known_sqft").desc(), "id")
w10 = Window.orderBy("dedup_rank_2", "id").rowsBetween(-3, -1)
w11 = Window.partitionBy("region", "city")
w_grp = Window.partitionBy("city").orderBy("sqft_bucket", "id").rowsBetween(-3, -1)
w_grp_2 = Window.partitionBy("city").orderBy("prev_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy("last_known_sqft_bucket", "id").rowsBetween(-3, -1)

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("opened_date").between("2023-08-01", "2025-03-10"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.initcap(F.trim("region")).alias("region_title"),
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .filter(F.upper("store_name").like("%IAM%"))
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("last_known_sqft", F.last("sqft", ignorenulls=True).over(w4))
    .withColumn("sqft_bucket", F.ntile(4).over(w5))
)

ranked_stores = (
    enriched
    .withColumn("dedup_rank_2", F.row_number().over(w6))
    .filter(F.col("dedup_rank_2") == 1)
)

metrics = (
    ranked_stores
    .select(
        "id",
        "city",
        "dedup_rank",
        "prev_sqft",
        "sqft_bucket",
        F.row_number().over(w7).alias("seq_no"),
        F.lag("prev_sqft", 1).over(w8).alias("prev_prev_sqft"),
        F.ntile(3).over(w9).alias("last_known_sqft_bucket"),
        F.avg("prev_sqft").over(w10).alias("prev_sqft_avg_3_prior"),
        F.round((F.col("sqft") / F.sum("sqft").over(w11)), 4).alias("sqft_share"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "city",
        "dedup_rank",
        "sqft_bucket",
        "prev_prev_sqft",
        "sqft_share",
        F.avg("prev_prev_sqft").over(w_grp).alias("prev_prev_sqft_avg_3_prior"),
        F.last("sqft_bucket", ignorenulls=True).over(w_grp_2).alias("last_known_sqft_bucket_2"),
        F.avg("prev_prev_sqft").over(w_recent).alias("prev_prev_sqft_avg_3_prior_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "sqft_bucket",
        "prev_prev_sqft",
        "sqft_share",
        "prev_prev_sqft_avg_3_prior",
        "prev_prev_sqft_avg_3_prior_2",
    )
)

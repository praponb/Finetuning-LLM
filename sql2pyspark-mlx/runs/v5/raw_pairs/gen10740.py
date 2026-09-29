from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("is_sg").desc(), "id")
w2 = Window.orderBy("is_sg", "id")
w3 = Window.partitionBy("customer_id").orderBy("is_sg", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("country", "customer_id")
w5 = Window.orderBy(F.col("country_max_is_sg").desc_nulls_last())
w6 = Window.partitionBy("country").orderBy("is_sg_bucket", "id").rowsBetween(-3, -1)
w7 = Window.orderBy(F.col("is_sg_avg_6").desc(), "id")
w8 = Window.orderBy(F.col("country_max_is_sg_dense_rank").desc(), "id")
w9 = Window.orderBy("seq_no", "id").rowsBetween(-3, -1)
w10 = Window.orderBy("country_max_is_sg_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("country") == "SG"))
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "country",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(3).over(w2).alias("is_sg_bucket"),
        F.avg("is_sg").over(w3).alias("is_sg_avg_6"),
        F.max("is_sg").over(w4).alias("country_max_is_sg"),
    )
)

with_windows_sessions = (
    with_windows
    .select(
        "id",
        "seq_no",
        F.dense_rank().over(w5).alias("country_max_is_sg_dense_rank"),
        F.avg("is_sg_bucket").over(w6).alias("is_sg_bucket_avg_3_prior"),
        F.ntile(4).over(w7).alias("is_sg_avg_6_bucket"),
    )
)

enriched = (
    with_windows_sessions
    .withColumn("country_max_is_sg_dense_rank_bucket", F.ntile(5).over(w8))
    .withColumn("country_max_is_sg_dense_rank_avg_3_prior", F.avg("country_max_is_sg_dense_rank").over(w9))
    .withColumn("cumulative_seq_no", F.sum("seq_no").over(w10))
)

filtered = (
    enriched
    .filter(F.col("country_max_is_sg_dense_rank_bucket").between(4, 5))
    .withColumn("country_max_is_sg_dense_rank_bucket_str", F.col("country_max_is_sg_dense_rank_bucket").cast("double"))
)

result = (
    filtered
    .rollup(
        F.when(F.col("country_max_is_sg_dense_rank_bucket") >= 7, "LARGE").otherwise("SMALL").alias("country_max_is_sg_dense_rank_bucket_tier"),
    )
    .agg(
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.min("is_sg_bucket_avg_3_prior").alias("min_is_sg_bucket_avg_3_prior"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
    )
)

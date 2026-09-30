from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("latency_ms", "id", "k")
w2 = Window.partitionBy("k").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("next_latency_ms_2", "id", "k").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_first())
w5 = Window.partitionBy("k").orderBy(F.col("next_latency_ms").desc_nulls_first(), "id", "k")
w6 = Window.partitionBy("k").orderBy(F.col("latency_ms_dense_rank").desc_nulls_first())
w_grp = Window.orderBy("latency_ms_dense_rank", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("k")
w9 = Window.partitionBy("k").orderBy(F.col("next_latency_ms_2_avg_2").asc_nulls_first())
w10 = Window.partitionBy("k").orderBy("latency_ms_dense_rank", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("k").orderBy("latency_ms", "id", "k").rowsBetween(-3, Window.currentRow)

flattened = (
    df_app_logs
    .select("id", "service", "host", "latency_ms", F.explode("context").alias("k", "v"))
)

base = (
    flattened
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "latency_ms",
        "k",
        "v",
        F.initcap(F.trim("service")).alias("service_title"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    latest
    .withColumn("next_latency_ms", F.lead("latency_ms", 1).over(w))
    .withColumn("next_latency_ms_2", F.lead("latency_ms", 1).over(w))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w2))
)

metrics = (
    scored
    .withColumn("next_latency_ms_2_avg_2", F.avg("next_latency_ms_2").over(w3))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w4))
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

scored_app_logs = (
    ranked
    .withColumn("latency_ms_dense_rank_rank", F.rank().over(w6))
    .withColumn("first_known_latency_ms_dense_rank", F.first("latency_ms_dense_rank", ignorenulls=True).over(w_grp))
)

result = (
    scored_app_logs
    .withColumn("k_count_first_known_latency_ms_dense_rank", F.count("first_known_latency_ms_dense_rank").over(w8))
    .withColumn("latency_ms_bucket", F.ntile(4).over(w))
    .withColumn("next_latency_ms_2_avg_2_rank", F.rank().over(w9))
    .withColumn("last_known_first_known_latency_ms_dense_rank", F.last("first_known_latency_ms_dense_rank", ignorenulls=True).over(w10))
    .withColumn("latency_ms_dense_rank_rank_avg_3", F.avg("latency_ms_dense_rank_rank").over(w11))
)

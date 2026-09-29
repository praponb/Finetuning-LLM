from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "attr_key").orderBy(F.col("latency_ms").desc_nulls_first())
w_grp = Window.partitionBy("level").orderBy("last_known_latency_ms", "id", "attr_key")
w4 = Window.partitionBy("host").orderBy(F.col("last_known_latency_ms").desc(), "id", "attr_key")
w5 = Window.orderBy("last_known_latency_ms", "id", "attr_key")
w6 = Window.partitionBy("level").orderBy("last_known_latency_ms", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("last_known_latency_ms_bucket", "id", "attr_key")
w8 = Window.partitionBy("host", "attr_key")
w9 = Window.partitionBy("level").orderBy("seq_no", "id", "attr_key").rowsBetween(-5, -1)
w10 = Window.orderBy("seq_no_2", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "v"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.last("latency_ms", ignorenulls=True).over(w_ordered).alias("last_known_latency_ms"),
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
    )
)

scored = (
    metrics
    .withColumn("prev_latency_ms", F.lag("latency_ms").over(w_grp))
    .withColumn("next_latency_ms_dense_rank", F.lead("latency_ms_dense_rank").over(w_grp))
    .withColumn("prev_latency_ms_dense_rank", F.lag("latency_ms_dense_rank").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("last_known_latency_ms_bucket", F.ntile(5).over(w4))
)

metrics_app_logs = (
    scored
    .withColumn("seq_no_2", F.row_number().over(w5))
    .withColumn("latency_ms_dense_rank_avg_3", F.avg("latency_ms_dense_rank").over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
)

scored_app_logs = (
    metrics_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_key",
        "latency_ms_dense_rank",
        "prev_latency_ms_dense_rank",
        "seq_no",
        "seq_no_2",
        "row_seq",
        F.round((F.col("latency_ms_dense_rank_avg_3") / F.sum("latency_ms_dense_rank_avg_3").over(w8)), 4).alias("latency_ms_dense_rank_avg_3_share"),
        F.avg("latency_ms_dense_rank").over(w9).alias("latency_ms_dense_rank_avg_5_prior"),
        F.ntile(4).over(w10).alias("seq_no_2_bucket"),
    )
)

result = (
    scored_app_logs
    .select(
        "id",
        "attr_key",
        "latency_ms_dense_rank",
        "seq_no",
        "seq_no_2",
        "row_seq",
        "latency_ms_dense_rank_avg_3_share",
        "latency_ms_dense_rank_avg_5_prior",
        "seq_no_2_bucket",
    )
)

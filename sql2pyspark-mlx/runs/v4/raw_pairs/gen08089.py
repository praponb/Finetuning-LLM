from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("created_at").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k")
w3 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k").orderBy(F.col("event_seq_3").desc_nulls_first(), "id", "k")
w5 = Window.partitionBy("k").orderBy(F.col("event_seq_3").desc_nulls_last())
w6 = Window.orderBy(F.col("event_seq_2").asc_nulls_first())
w7 = Window.partitionBy("k").orderBy(F.col("event_seq").asc_nulls_last())
w8 = Window.partitionBy("k")
w9 = Window.orderBy(F.col("event_seq_2_dense_rank").asc_nulls_first())
w10 = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w11 = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w12 = Window.orderBy(F.col("event_seq_2").asc_nulls_last())

lines = df_carts.select("id", "created_at", F.explode("attributes").alias("k", "value"))

with_windows = (
    lines
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("event_seq_2", F.row_number().over(w2))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w3))
    .withColumn("event_seq_3", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

kept_keys = (
    ranked
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.hour("created_at").between(10, 18))
        .select("id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("event_seq_3_rank", F.rank().over(w5))
    .withColumn("event_seq_2_dense_rank", F.dense_rank().over(w6))
    .withColumn("event_seq_dense_rank", F.dense_rank().over(w7))
    .withColumn("event_seq_2_share", F.round((F.col("event_seq_2") / F.sum("event_seq_2").over(w8)), 4))
)

metrics = (
    scored
    .select(
        "*",
        F.round((F.col("event_seq_dense_rank") / F.sum("event_seq_dense_rank").over(w8)), 4).alias("event_seq_dense_rank_share"),
        F.rank().over(w9).alias("event_seq_2_dense_rank_rank"),
        F.min("event_seq_2_dense_rank").over(w8).alias("k_min_event_seq_2_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "*",
        F.sum("event_seq_3_rank").over(w10).alias("event_seq_3_rank_sum_2h"),
        F.count("event_seq_2_dense_rank").over(w11).alias("event_seq_2_dense_rank_count_1h"),
        F.percent_rank().over(w12).alias("event_seq_2_percent_rank"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("k", "host").orderBy(F.col("log_ts").desc(), "id", "k")
w_hist_2 = Window.partitionBy("level").orderBy(F.col("log_ts").desc(), "id", "k")
w3 = Window.partitionBy("level", "host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k").orderBy(F.col("log_ts").desc(), "id", "k")
w5 = Window.partitionBy("host", "level").orderBy(F.col("row_seq").desc_nulls_last(), "id", "k")
w6 = Window.orderBy(F.col("row_seq").desc(), "id", "k")
w7 = Window.orderBy("seq_no", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("k")

flattened = (
    df_app_logs
    .select("id", "host", "log_ts", "level", F.explode("context").alias("k", "v"))
)

enriched = (
    flattened
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist_2))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .withColumn("event_seq_2", F.row_number().over(w6))
    .withColumn("last_known_k", F.last("k", ignorenulls=True).over(w7))
    .withColumn("k_sum_event_seq", F.sum("event_seq").over(w_grp))
)

result = (
    with_windows
    .groupBy("host", "level")
    .agg(
        F.max(F.when(F.col("host") == "h2", F.col("k_sum_event_seq"))).alias("h2_max_k_sum_event_seq"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("level").desc())
)

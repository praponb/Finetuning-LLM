from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "service").orderBy(F.col("log_ts").desc(), "id", "k")
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("host").orderBy(F.col("rank_in_group").desc(), "id", "k")
w_running = Window.partitionBy("service").orderBy("rank_in_group", "id", "k")
w6 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w7 = Window.partitionBy("host", "service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("k", "value"),
    )
)

scored = lines.withColumn("first_known_k", F.first("k", ignorenulls=True).over(w))

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = latest.withColumn("last_known_k", F.last("k", ignorenulls=True).over(w3))

with_windows_app_logs = (
    with_windows
    .filter(F.col("service").isin(["search", "checkout"]))
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "value",
        "first_known_k",
        "last_known_k",
        F.row_number().over(w_part).alias("row_seq"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w6).alias("row_seq_2"),
        F.first("k", ignorenulls=True).over(w7).alias("first_known_k_2"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "value",
        "first_known_k",
        "last_known_k",
        "row_seq",
        "seq_no",
        F.when(F.col("k").rlike("^(version|region)$"), "CORE").when(F.col("k").rlike("^(trace)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("k_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first(), "service")
)

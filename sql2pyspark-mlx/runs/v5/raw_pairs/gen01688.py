from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("level").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("row_seq", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("service")
w5 = Window.orderBy(F.col("row_seq").asc_nulls_last())
w6 = Window.partitionBy("service").orderBy(F.col("auth_rows").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "v"),
    )
)

with_windows = lines.withColumn("row_seq", F.row_number().over(w_ordered))

cleaned = (
    with_windows
    .filter((F.col("log_ts") > F.lit("2025-06-01 08:00:00").cast("timestamp")) & (F.col("log_ts") < "2024-05-01"))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "attr_key",
        "row_seq",
        F.concat("level", F.lit(" "), "host").alias("level_host"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_clean"),
    )
)

metrics = (
    cleaned
    .filter(F.col("service").isin(["checkout", "billing", "auth"]))
    .withColumn("last7_min_row_seq", F.min("row_seq").over(w2))
    .withColumn("last7_sum_row_seq", F.sum("row_seq").over(w2))
    .withColumn("last7_count_row_seq", F.count("row_seq").over(w2))
    .withColumn("row_seq_avg_15m", F.avg("row_seq").over(w3))
)

scored = (
    metrics
    .select(
        "id",
        "service",
        "host",
        "attr_key",
        "row_seq",
        "level_host",
        "last7_sum_row_seq",
        "last7_count_row_seq",
        "row_seq_avg_15m",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w4)), 4).alias("row_seq_share"),
        F.cume_dist().over(w5).alias("row_seq_cume_dist"),
    )
)

summary = (
    scored
    .groupBy("service", "host")
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.sum(F.when(F.col("host") == "h3", F.col("last7_sum_row_seq"))).alias("h3_total_last7_sum_row_seq"),
    )
    .withColumn("auth_rows_rank", F.rank().over(w6))
)

result = (
    summary
    .select(
        "service",
        "host",
        "auth_rows",
        "h3_total_last7_sum_row_seq",
        F.when(F.col("host").rlike("^(h3|h1)$"), "CORE").when(F.col("host").rlike("^(h2|h4)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("host_zone"),
    )
    .orderBy(F.col("host").desc(), F.col("host_zone").desc())
)

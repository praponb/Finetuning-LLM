from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

with_windows = (
    flattened
    .withColumn("status_code_sum_1h", F.sum("status_code").over(w))
    .withColumn("status_code_sum_1h_2", F.sum("status_code").over(w2))
    .withColumn("status_code_count_2h", F.count("status_code").over(w3))
)

cleaned = (
    with_windows
    .filter(F.col("status_code_sum_1h").isNull())
    .select(
        "service",
        "log_ts",
        "status_code",
        "k",
        "status_code_sum_1h",
        "status_code_count_2h",
        (F.col("status_code_sum_1h") - F.col("status_code_count_2h")).alias("status_code_sum_1h_minus_status_code_count_2h"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("status_code_sum_1h") >= 1603, "LARGE").otherwise("SMALL").alias("status_code_sum_1h_tier"),
    )
    .agg(
        F.percentile_approx("status_code_sum_1h", 0.9).alias("status_code_sum_1h_p90"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_count"),
    )
    .orderBy(F.col("hour_window").desc())
)

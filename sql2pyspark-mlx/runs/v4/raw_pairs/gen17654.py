from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("session_no").desc(), "id", "key")

lines = (
    df_app_logs
    .select("id", "service", "log_ts", F.explode("context").alias("key", "value"))
)

gaps = (
    lines
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

totals = (
    latest
    .cube(
        "key",
        F.when(F.col("session_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
        "service",
    )
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count("*").alias("row_count"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("row_count") > 1)
)

result = (
    totals
    .select(
        "key",
        "session_no_tier",
        "service",
        "num_keys",
        "row_count",
        "span_minutes",
    )
    .orderBy(F.col("session_no_tier").desc(), "span_minutes")
)

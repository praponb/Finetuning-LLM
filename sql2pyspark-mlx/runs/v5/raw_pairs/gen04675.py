from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "host").orderBy(F.col("log_ts").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts")
w3 = Window.orderBy("log_ts", "id", "key")
w4 = Window.partitionBy("key").orderBy(F.col("days_since_log_ts").desc(), "id", "key")
w5 = Window.partitionBy("key").orderBy("days_since_log_ts", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("key", "attr_value"),
    )
)

deduped = (
    lines
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "log_ts", "key", "attr_value")
)

ranked = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "log_ts", "key", "attr_value")
)

cleaned = (
    ranked
    .filter(F.to_date("log_ts") < "2023-04-17")
    .withColumn("days_since_log_ts", F.datediff(F.current_date(), "log_ts"))
    .withColumn("key_filled", F.coalesce("key", F.lit("OTHER")))
)

result = (
    cleaned
    .filter(F.col("key") == "version")
    .select(
        "id",
        "key",
        "days_since_log_ts",
        F.lead("days_since_log_ts", 2).over(w3).alias("next_days_since_log_ts"),
        F.ntile(10).over(w4).alias("days_since_log_ts_bucket"),
        F.ntile(5).over(w5).alias("days_since_log_ts_bucket_2"),
    )
)

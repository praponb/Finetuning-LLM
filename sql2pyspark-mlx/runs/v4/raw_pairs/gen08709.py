from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc(), "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

ranked = (
    lines
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked_app_logs = (
    ranked
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

result = (
    ranked_app_logs
    .groupBy("host")
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.max(F.when(F.col("host") == "h3", F.col("status_code"))).alias("h3_max_status_code"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("host").desc_nulls_first())
)

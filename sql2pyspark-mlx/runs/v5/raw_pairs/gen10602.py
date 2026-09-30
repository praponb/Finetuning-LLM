from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("log_ts").desc(), "id", "k")
w3 = Window.partitionBy("level", "k").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("k").orderBy(F.col("status_code").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

enriched = (
    exploded
    .filter(~F.col("service").isin(["search", "auth"]))
    .select(
        "*",
        F.avg("status_code").over(w).alias("status_code_avg_3"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "k",
        "value",
        "status_code_avg_3",
        F.when(F.col("k").rlike("^(version|trace)$"), "CORE").when(F.col("k").rlike("^(region)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("k_zone"),
    )
)

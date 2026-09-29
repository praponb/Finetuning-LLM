from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "attr_key")
w2 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("attr_key", "service").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w4 = Window.partitionBy("service", "attr_key").orderBy(F.col("latest_rank").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

latest = (
    flattened
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    deduped
    .filter(F.col("attr_key").isNotNull())
    .select("*", F.concat("attr_key", F.lit(" "), "service").alias("attr_key_service"))
)

latest_app_logs = (
    base
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

deduped_app_logs = (
    latest_app_logs
    .withColumn("rank_in_group_2", F.row_number().over(w4))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

result = (
    deduped_app_logs
    .select(
        "id",
        "status_code",
        "attr_key",
        "attr_value",
        F.when(F.col("attr_key").rlike("^(region|version)$"), "GROUP_A").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("status_code").desc())
)

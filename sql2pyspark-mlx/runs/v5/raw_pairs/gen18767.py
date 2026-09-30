from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "attr_key").orderBy("status_code_whole", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("attr_key").orderBy("last7_sum_status_code_whole", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy(F.col("last3_max_last7_min_status_code_whole").desc_nulls_last(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("level").isin(["WARN", "ERROR"]))
    .select(
        "id",
        "host",
        "status_code",
        "attr_key",
        F.round("status_code", 0).alias("status_code_rounded"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("attr_key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "host",
        "attr_key",
        "resolved_attr_key",
        F.ceil("status_code").alias("status_code_whole"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_clean"),
    )
)

enriched = (
    valid_app_logs
    .withColumn("last7_avg_status_code_whole", F.avg("status_code_whole").over(w))
    .withColumn("last7_min_status_code_whole", F.min("status_code_whole").over(w))
    .withColumn("last7_sum_status_code_whole", F.sum("status_code_whole").over(w))
)

scored = (
    enriched
    .filter(F.col("attr_key") != "region")
    .select(
        "id",
        "attr_key",
        "resolved_attr_key",
        "last7_avg_status_code_whole",
        "last7_min_status_code_whole",
        "last7_sum_status_code_whole",
        F.sum("last7_min_status_code_whole").over(w_grp).alias("last3_sum_last7_min_status_code_whole"),
        F.avg("last7_min_status_code_whole").over(w_grp).alias("last3_avg_last7_min_status_code_whole"),
        F.max("last7_min_status_code_whole").over(w_grp).alias("last3_max_last7_min_status_code_whole"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .select(
        "id",
        "attr_key",
        "resolved_attr_key",
        "last7_avg_status_code_whole",
        "last7_min_status_code_whole",
        "last3_sum_last7_min_status_code_whole",
        "last3_avg_last7_min_status_code_whole",
        "last3_max_last7_min_status_code_whole",
    )
    .orderBy(F.col("last7_min_status_code_whole").desc_nulls_first())
)

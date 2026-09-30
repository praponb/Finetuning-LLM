from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code_high").desc_nulls_first(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

unpacked = (
    lines
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        "key",
        "v",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

deduped = (
    unpacked
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    deduped
    .select("service", "id")
    .intersectAll(
        df_app_logs
        .filter(F.hour("log_ts").between(12, 20))
        .select("service", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("service", "flag_bit8", "status_code_high", "rank_in_group")
    .orderBy("rank_in_group", F.col("flag_bit8").desc())
)

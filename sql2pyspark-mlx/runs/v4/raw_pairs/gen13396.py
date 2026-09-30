from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "key").orderBy("status_code")
w2 = Window.partitionBy("level", "host").orderBy(F.col("status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

deduped = (
    lines
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_app_logs = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_app_logs
    .select(
        "id",
        "status_code",
        "key",
        "attr_value",
        "latest_rank",
        F.when(F.col("key").rlike("^(trace|region)$"), "EXTENDED").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(F.col("key_zone").desc_nulls_first())
)

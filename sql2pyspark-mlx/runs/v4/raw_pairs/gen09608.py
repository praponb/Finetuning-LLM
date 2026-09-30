from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.orderBy(F.col("status_code_share").desc_nulls_first())
w3 = Window.orderBy(F.col("attr_key_sum_status_code").desc())

lines = df_app_logs.select("id", "status_code", F.explode("context").alias("attr_key", "v"))

kept_keys = (
    lines
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("level") == "INFO")
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("attr_key_sum_status_code", F.sum("status_code").over(w))
)

metrics_app_logs = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w2).alias("status_code_share_percent_rank"),
        F.rank().over(w3).alias("attr_key_sum_status_code_rank"),
    )
)

cleaned = (
    metrics_app_logs
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "status_code",
        "attr_key",
        "v",
        "status_code_share",
        "attr_key_sum_status_code",
        "attr_key_sum_status_code_rank",
        F.greatest("attr_key_sum_status_code", "status_code_share").alias("greatest_attr_key_sum_status_code_status_code_share"),
        F.col("status_code").cast("double").alias("status_code_num"),
    )
)

scored = (
    cleaned
    .withColumn("status_code_share_2", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("attr_key_sum_status_code_share", F.round((F.col("attr_key_sum_status_code") / F.sum("attr_key_sum_status_code").over(w)), 4))
    .withColumn("attr_key_count_attr_key_sum_status_code_rank", F.count("attr_key_sum_status_code_rank").over(w))
)

with_windows = (
    scored
    .withColumn("attr_key_sum_status_code_share_2", F.sum("status_code_share_2").over(w))
    .withColumn("attr_key_count_attr_key_sum_status_code_share", F.count("attr_key_sum_status_code_share").over(w))
)

result = (
    with_windows
    .select(
        "id",
        "status_code",
        "attr_key_sum_status_code",
        "attr_key_sum_status_code_rank",
        "status_code_num",
        "status_code_share_2",
        "attr_key_sum_status_code_share",
        "attr_key_count_attr_key_sum_status_code_rank",
        "attr_key_sum_status_code_share_2",
    )
    .orderBy(F.col("attr_key_sum_status_code_share_2").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w_running = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("attr_key", "service")
w_recent = Window.partitionBy("service").orderBy(F.col("status_code_percent_rank").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

base = (
    lines
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key").eqNullSafe(F.lit("region"))))
    .withColumn("attr_key_lower", F.lower("attr_key"))
)

metrics = (
    base
    .select(
        "id",
        "service",
        "v",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.percent_rank().over(w_running).alias("status_code_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share_2"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_3"),
        F.percent_rank().over(w_running).alias("status_code_percent_rank_2"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("status_code_percent_rank_cume_dist"),
    )
)

result = (
    scored
    .select(
        "service",
        "v",
        "status_code_share",
        "status_code_percent_rank",
        "status_code_percent_rank_2",
        "status_code_percent_rank_cume_dist",
        F.when(F.col("service").rlike("^(auth|billing)$"), "EXTENDED").when(F.col("service").rlike("^(search|checkout)$"), "CORE").otherwise("GROUP_A").alias("service_zone"),
    )
)

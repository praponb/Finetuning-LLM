from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w_hist = Window.partitionBy("service").orderBy(F.col("status_code_share_2").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("has_fault") < 1)
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "attr_value",
        F.trim("service").alias("clean_service"),
        F.when(F.col("status_code") >= 467, "High").when(F.col("status_code") >= 252, "Medium").otherwise("Low").alias("status_code_band"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_2"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
    )
)

scored = (
    enriched
    .filter(F.col("status_code_share_2").isNotNull())
    .select(
        "id",
        "status_code",
        "status_code_band",
        "status_code_share",
        "status_code_share_2",
        F.percent_rank().over(w_hist).alias("status_code_share_2_percent_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "status_code",
        "status_code_band",
        "status_code_share",
        "status_code_share_2_percent_rank",
    )
)

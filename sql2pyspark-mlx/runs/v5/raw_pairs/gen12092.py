from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("is_h3").desc_nulls_last())
w2 = Window.partitionBy("service", "host").orderBy("is_h3_percent_rank_per_is_h3")
w3 = Window.partitionBy("host").orderBy(F.col("is_h3_percent_rank_per_is_h3").desc())
w4 = Window.partitionBy("service")
w5 = Window.orderBy(F.col("is_h3_percent_rank").desc_nulls_last())
w6 = Window.partitionBy("host").orderBy(F.col("is_h3").desc_nulls_last())

flattened = (
    df_app_logs
    .select("id", "service", "host", F.explode_outer("context").alias("k", "v"))
)

cleaned = (
    flattened
    .filter(F.col("k").isNotNull() & F.col("service").isin(["auth", "billing"]))
    .withColumn("is_h3", F.when(F.col("host") == "h3", 1).otherwise(0))
)

enriched = cleaned.select("*", F.percent_rank().over(w_part).alias("is_h3_percent_rank"))

kept_keys = (
    enriched
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("host") == "h4")
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("k") == "region")
    .withColumn("is_h3_percent_rank_per_is_h3", (F.col("is_h3_percent_rank") / F.nullif("is_h3", F.lit(0))))
    .withColumn("service_host_key", F.concat_ws("-", "service", "host"))
)

with_windows = (
    filtered
    .select(
        "id",
        "service",
        "host",
        "v",
        "is_h3_percent_rank",
        "is_h3_percent_rank_per_is_h3",
        F.rank().over(w2).alias("is_h3_percent_rank_per_is_h3_rank"),
        F.dense_rank().over(w3).alias("is_h3_percent_rank_per_is_h3_dense_rank"),
        F.count("is_h3_percent_rank_per_is_h3").over(w4).alias("service_count_is_h3_percent_rank_per_is_h3"),
        F.rank().over(w5).alias("is_h3_percent_rank_rank"),
        F.rank().over(w6).alias("is_h3_rank"),
    )
)

result = (
    with_windows
    .select(
        "service",
        F.col("is_h3_percent_rank").alias("IS_H3_PERCENT_RANK_VAL"),
        F.col("is_h3_percent_rank_per_is_h3").alias("IS_H3_PERCENT_RANK_PER_IS_H3"),
    )
    .unpivot(
        ids=["service"],
        values=["IS_H3_PERCENT_RANK_VAL", "IS_H3_PERCENT_RANK_PER_IS_H3"],
        variableColumnName="metric",
        valueColumnName="val",
    )
)

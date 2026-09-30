from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w_ordered = Window.partitionBy("k").orderBy(F.col("status_code_scaled").desc_nulls_first())
w3 = Window.orderBy("status_code_scaled", "id", "k")
w4 = Window.partitionBy("k").orderBy("latency_ms", "id", "k").rowsBetween(-2, -1)
w5 = Window.partitionBy("k")
w6 = Window.partitionBy("k").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select("id", "latency_ms", "status_code", F.explode("context").alias("k", "v"))
)

base = (
    exploded
    .filter((F.col("k").eqNullSafe(F.lit("version"))) & F.col("status_code").between(282, 442))
    .select("*", F.round((F.col("status_code") * 100), 2).alias("status_code_scaled"))
)

latest = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "latency_ms", "k", "v", "status_code_scaled")
)

enriched = (
    latest
    .select(
        "*",
        F.percent_rank().over(w_ordered).alias("status_code_scaled_percent_rank"),
        F.lag("latency_ms", 2).over(w3).alias("prev_latency_ms"),
        F.avg("latency_ms").over(w4).alias("latency_ms_avg_2_prior"),
        F.max("status_code_scaled").over(w5).alias("k_max_status_code_scaled"),
        F.sum("status_code_scaled").over(w6).alias("cumulative_status_code_scaled"),
    )
)

valid = (
    enriched
    .filter(F.col("k_max_status_code_scaled") >= 70)
    .select(
        "*",
        F.when(F.col("k_max_status_code_scaled") >= 68, "High").when(F.col("k_max_status_code_scaled") >= 34, "Medium").otherwise("Low").alias("k_max_status_code_scaled_level"),
    )
)

result = (
    valid
    .select(
        "id",
        "k",
        "v",
        "status_code_scaled_percent_rank",
        "prev_latency_ms",
        "latency_ms_avg_2_prior",
        "k_max_status_code_scaled",
        "k_max_status_code_scaled_level",
    )
    .orderBy(F.col("id").desc())
)

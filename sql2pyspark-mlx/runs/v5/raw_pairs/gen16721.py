from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code_high").asc_nulls_first(), "id", "k")
w2 = Window.orderBy(F.col("is_checkout").desc(), "id", "k")
w3 = Window.orderBy("is_checkout", "id", "k")
w4 = Window.partitionBy("service", "k").orderBy("is_checkout", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("service").orderBy("is_checkout", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("is_checkout", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

unpacked = (
    exploded
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "service",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

latest = (
    unpacked
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "service", "k")
)

cleaned = (
    latest
    .filter(F.col("service") == "checkout")
    .select("*", F.initcap(F.trim("k")).alias("k_title"))
)

valid = (
    cleaned
    .filter(F.col("k").isNotNull())
    .select("*", F.initcap(F.trim("k")).alias("k_title_2"))
)

cleaned_app_logs = (
    valid
    .filter(F.col("k") == "region")
    .select(
        "id",
        "service",
        "k",
        "k_title",
        "k_title_2",
        F.concat_ws("-", "service", "k").alias("service_k_key"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

filtered = (
    cleaned_app_logs
    .filter(F.col("service") == "billing")
    .withColumn("is_checkout", F.when(F.col("service") == "checkout", 1).otherwise(0))
)

result = (
    filtered
    .filter(F.col("service") != "checkout")
    .select(
        "*",
        F.ntile(10).over(w2).alias("is_checkout_bucket"),
        F.lead("is_checkout", 1).over(w3).alias("next_is_checkout"),
        F.first("is_checkout", ignorenulls=True).over(w4).alias("first_known_is_checkout"),
        F.first("k", ignorenulls=True).over(w5).alias("first_known_k"),
        F.first("k", ignorenulls=True).over(w6).alias("first_known_k_2"),
    )
)

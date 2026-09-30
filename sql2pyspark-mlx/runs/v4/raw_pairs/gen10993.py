from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("service", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

base = (
    flattened
    .filter(F.col("k") != "trace")
    .select(
        "service",
        "log_ts",
        "status_code",
        "k",
        F.length("service").alias("service_length"),
    )
)

matched_keys = (
    base
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") != 318)
        .select("service")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("status_code_sum_15m", F.sum("status_code").over(w))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_code") >= 450, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.max("log_ts").alias("last_seen"),
        F.round(F.avg("status_code_share"), 2).alias("avg_status_code_share"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count_2"),
        F.sum(F.when(F.col("service") == "checkout", F.col("status_code"))).alias("checkout_total_status_code"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
    )
    .orderBy(F.col("status_code_tier").desc_nulls_last())
)

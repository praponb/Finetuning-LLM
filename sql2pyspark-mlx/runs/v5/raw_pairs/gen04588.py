from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

valid = (
    flattened
    .filter(F.col("effective_from").between("2025-03-01", "2025-06-12"))
    .select(
        "*",
        F.date_trunc("YEAR", "effective_from").alias("effective_from_period"),
        F.when(F.col("plan") == "pro", 1).otherwise(0).alias("is_pro"),
    )
)

metrics = valid.withColumn("plan_stddev_samp_is_pro", F.stddev_samp("is_pro").over(w))

remaining_keys = (
    metrics
    .select("plan", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
        .select("plan", "id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .rollup("plan", F.date_trunc("year", "effective_from_period").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.max("effective_from_period").alias("last_seen"),
    )
    .filter(F.col("distinct_ids") >= 1)
)

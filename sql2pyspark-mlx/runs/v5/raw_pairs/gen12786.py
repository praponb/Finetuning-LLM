from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id", "attr_key")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

ranked = (
    lines
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    ranked
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 16:00:00"))) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        "v",
        "rn",
        F.date_format("created_at", "MMM yyyy").alias("created_at_label"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("attr_key").isin(["gift", "utm_source"]) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        "v",
        "rn",
        "created_at_label",
        "attr_key_clean",
        F.upper("attr_key").alias("attr_key_upper"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean_2"),
    )
)

result = (
    valid
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["customer_id", "activity_day"], ["customer_id"]],
        "customer_id",
        "activity_day",
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "utm_source", 1)).alias("utm_source_rows"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_count"),
    )
    .orderBy(F.col("utm_source_rows").desc_nulls_first())
)

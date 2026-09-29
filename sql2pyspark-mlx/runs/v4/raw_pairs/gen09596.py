from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("k", "value"),
    )
)

remaining_keys = (
    lines
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.hour("created_at").between(6, 9))
        .select("id")
    )
)

verified = (
    lines
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.country"), F.col("d.name"))
)

base = (
    joined
    .filter(F.col("k") == "weight_kg")
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "value",
        "name",
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
        F.regexp_replace("name", " ", "").alias("name_slug"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

valid = (
    base
    .filter(F.col("k").isNotNull() & (F.length("name") < 11))
    .withColumn("name_normalized", F.upper(F.trim("name")))
    .withColumn("k_name", F.concat("k", F.lit("/"), "name"))
)

result = (
    valid
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets([["activity_day", "k"], ["activity_day"], []], "activity_day", "k")
    .agg(
        F.count(F.when(F.col("k") == "device", 1)).alias("device_count"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("k") == "weight_kg", 1)).alias("weight_kg_count"),
        F.count("*").alias("event_count_2"),
        F.count(F.when(F.col("k") == "weight_kg", 1)).alias("weight_kg_count_2"),
    )
    .orderBy(F.col("event_count_2").desc())
)

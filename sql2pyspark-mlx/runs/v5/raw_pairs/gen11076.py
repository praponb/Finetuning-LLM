from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

exploded_carts = (
    exploded
    .select("id", "created_at", "attr_key", F.explode("coupon_codes").alias("element"))
)

valid = (
    exploded_carts
    .filter(F.col("created_at") > F.to_date(F.lit("2025-03-02")))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
    .withColumn("created_at_period", F.date_trunc("YEAR", "created_at"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

valid_carts = (
    valid
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-01")))
    .select(
        "id",
        "created_at",
        "attr_key",
        "element",
        "created_at_label",
        "attr_key_clean",
        F.length("element").alias("element_length"),
    )
)

result = (
    valid_carts
    .groupBy(
        F.when(F.col("element_length") >= 68, "HIGH").otherwise("LOW").alias("element_length_tier"),
    )
    .agg(
        F.round(F.sum("element_length") / F.nullif(F.count(F.when(F.col("attr_key") == "device", 1)), F.lit(0)), 2).alias("element_length_per_device"),
        F.max("created_at").alias("last_created_at"),
        F.max("created_at").alias("last_created_at_2"),
    )
    .orderBy("element_length_tier")
)

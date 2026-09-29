from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("tag"))

filtered = (
    flattened
    .filter(F.col("tag").isNotNull())
    .withColumn("tag_lower", F.lower("tag"))
)

remaining_keys = (
    filtered
    .select("id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") >= "2024-11-01")
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "tag",
        "tag_lower",
        F.when(F.col("tag").rlike("^(SPRING10|VIP)$"), "GROUP_B").otherwise("GROUP_A").alias("tag_zone"),
    )
)

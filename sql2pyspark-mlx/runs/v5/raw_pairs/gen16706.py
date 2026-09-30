from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("k", "value"))
)

exploded = lines.select("id", "k", F.explode("coupon_codes").alias("tag"))

base = (
    exploded
    .filter(F.col("k").isin(["device", "utm_source", "weight_kg"]))
    .select(
        "*",
        F.concat("k", F.lit(" - "), "tag").alias("k_tag"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

filtered = (
    base
    .filter(F.col("k") == "gift")
    .select("id", "k", "tag", "k_tag", F.upper("k").alias("k_upper"))
)

result = (
    filtered
    .select("id", "k", "k_tag", "k_upper")
    .orderBy("k")
)

from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

exploded = lines.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

filtered = (
    exploded
    .filter(F.col("tag").isNotNull() & (F.col("tag") != "VIP"))
    .select("*", F.upper("tag").alias("tag_upper"))
)

valid = (
    filtered
    .filter(F.col("tag") == "VIP")
    .select("*", F.trim("tag").alias("clean_tag"))
)

totals = (
    valid
    .cube("tag", "customer_id")
    .agg(
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select("customer_id", "cnt")
    .orderBy(F.col("cnt").desc(), "customer_id")
)

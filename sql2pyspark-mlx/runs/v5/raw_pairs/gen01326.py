from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

flattened = exploded.select("qty", "price", F.explode("coupon_codes").alias("element"))

base = (
    flattened
    .filter(F.col("element").isin(["WELCOME5", "VIP", "SPRING10"]))
    .withColumn("price_per_qty", F.round((F.col("price") / F.col("qty")), 2))
    .withColumn("resolved_element", F.coalesce("element", F.lit("OTHER")))
)

base_carts = (
    base
    .filter(~F.col("element").eqNullSafe(F.lit("WELCOME5")))
    .select(
        "qty",
        "price",
        "element",
        "price_per_qty",
        F.round("price_per_qty", 0).alias("price_per_qty_rounded"),
    )
)

filtered = (
    base_carts
    .filter(F.col("element").isNotNull() & (F.col("price_per_qty_rounded") > 61))
    .select(
        "price",
        "element",
        "price_per_qty",
        "price_per_qty_rounded",
        F.floor("price_per_qty").alias("price_per_qty_whole"),
    )
)

result = (
    filtered
    .select(
        "element",
        "price_per_qty",
        "price_per_qty_rounded",
        F.when(F.col("element").rlike("^(VIP|BUNDLE)$"), "CORE").when(F.col("element").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").otherwise("GROUP_A").alias("element_zone"),
    )
)

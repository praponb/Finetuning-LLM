from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

with_dims = (
    exploded
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.city"), F.col("p.name"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.age"))
)

valid = (
    enriched
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "qty",
        "price",
        "promo",
        "city",
        "name",
        F.upper(F.trim("name")).alias("name_normalized"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("price") < 170)
    .select(
        "id",
        "customer_id",
        "qty",
        "promo",
        "city",
        "name_normalized",
        "city_filled",
        F.when(F.col("promo").isin(["BUNDLE", "WELCOME5"]), "Tier 1").otherwise("Other").alias("promo_group"),
    )
)

result = (
    filtered
    .select(
        "id",
        "qty",
        "promo",
        "city",
        "name_normalized",
        "city_filled",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
)

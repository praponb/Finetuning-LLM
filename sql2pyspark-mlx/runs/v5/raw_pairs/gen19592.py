from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

cleaned = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("promo").isNotNull())
    .withColumn("greatest_price_product_id", F.greatest("price", "product_id"))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("N/A")))
)

valid = (
    cleaned
    .filter(F.col("promo").isin(["VIP", "WELCOME5"]))
    .select(
        "created_at",
        "product_id",
        "qty",
        "price",
        "promo",
        "greatest_price_product_id",
        F.hour("created_at").alias("created_at_hour"),
        F.when(F.col("promo") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

valid_carts = (
    valid
    .filter(F.col("promo").isNotNull() & F.col("is_spring10").between(62, 67))
    .withColumn("promo_prefix", F.substring("promo", 1, 2))
)

base = (
    valid_carts
    .filter(F.col("promo").isNotNull() & (~F.col("promo").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "created_at",
        "product_id",
        "qty",
        "price",
        "promo",
        "greatest_price_product_id",
        "is_spring10",
        "promo_prefix",
        F.col("is_spring10").cast("double").alias("is_spring10_str"),
        (F.col("created_at_hour") * F.col("is_spring10")).alias("created_at_hour_times_is_spring10"),
    )
)

result = (
    base
    .select(
        "promo",
        F.col("price").alias("PRICE_VAL"),
        F.col("greatest_price_product_id").alias("GREATEST_PRICE_PRODUCT_ID_VAL"),
        F.col("created_at_hour_times_is_spring10").alias("CREATED_AT_HOUR_TIMES_IS_SPRING10"),
        "is_spring10_str",
    )
    .unpivot(
        ids=["promo"],
        values=["PRICE_VAL", "GREATEST_PRICE_PRODUCT_ID_VAL", "CREATED_AT_HOUR_TIMES_IS_SPRING10", "is_spring10_str"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

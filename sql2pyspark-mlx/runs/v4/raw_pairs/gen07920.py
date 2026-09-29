from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("brand", "category")
    .intersectAll(
        df_products
        .filter(F.col("brand") == "Globex")
        .select("brand", "category")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("price_tier", F.when(F.col("price") >= 1080, "LARGE").otherwise("SMALL"))
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 350, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["price_tier", "stock_qty_tier"], ["price_tier"]],
        "price_tier",
        "stock_qty_tier",
    )
    .agg(
        F.countDistinct("brand").alias("distinct_brands"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("category") == "Sports", 1)), F.lit(0)), 2).alias("cost_per_sports"),
        F.count("*").alias("cnt"),
        F.grouping("price_tier").alias("grouping_level"),
    )
    .orderBy(F.col("distinct_brands").asc_nulls_last())
)

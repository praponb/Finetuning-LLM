from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-07-24")))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM"))
)

enriched = (
    valid
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .filter(~F.col("b.city").isin(["Bangkok", "Singapore"]))
    .select("src.*", F.col("b.city"), F.col("b.region"), F.col("b.sqft"))
)

filtered = (
    enriched
    .filter(F.col("city").isNotNull() & (F.col("region") != "West"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "city",
        "region",
        "sqft",
        F.concat("region", F.lit(" "), "city").alias("region_city"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([7, 8, 9, 10]))
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_store_sales = (
    kept_rows
    .filter(F.col("region").isNotNull() & (F.col("sale_date") >= F.to_date(F.lit("2023-02-14"))))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "city",
        "sqft",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
        (F.col("revenue") - F.col("sqft")).alias("revenue_minus_sqft"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

joined = (
    filtered_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.product_id",
        "src.units",
        "src.city",
        "src.sqft",
        "src.revenue_minus_sqft",
        "src.city_filled",
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)

summary = (
    joined
    .groupBy(
        F.when(F.col("revenue_minus_sqft") >= 79, "LARGE").otherwise("SMALL").alias("revenue_minus_sqft_tier"),
        F.when(F.col("sqft") >= 15346, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("cost")).otherwise(0)).alias("tokyo_cost"),
    )
    .filter(F.col("tokyo_cost") < 1000000)
)

result = (
    summary
    .select(
        "revenue_minus_sqft_tier",
        "sqft_tier",
        "num_cities",
        "num_cities_2",
        "tokyo_cost",
    )
    .orderBy("revenue_minus_sqft_tier", "tokyo_cost")
)

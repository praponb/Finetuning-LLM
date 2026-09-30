from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") == "JP"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat_ws("-", "country", "supplier_name").alias("country_supplier_name_key"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isin(["Acme", "Stark", "Initech"])), "left_anti")
)

matched_keys = (
    not_flagged
    .select("city", "country")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") > 2.2)
        .select("city", "country")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("city").isNotNull() & F.col("supplier_name").like("%Supply%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "country_supplier_name_key",
        F.ceil("rating").alias("rating_whole"),
    )
)

result = (
    valid
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.6, "LARGE").otherwise("SMALL"))
    .groupingSets([["rating_tier", "city"], ["rating_tier"]], "rating_tier", "city")
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("rating_per_singapore"),
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
        F.avg("rating").alias("avg_rating"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.grouping("rating_tier").alias("gid"),
    )
    .orderBy(F.col("num_cities").desc_nulls_first())
)

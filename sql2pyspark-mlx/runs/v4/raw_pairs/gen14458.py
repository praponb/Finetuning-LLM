from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("rating") > 2.8)
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", ""))
    .withColumn("city_filled", F.coalesce("city", F.lit("OTHER")))
)

matched_keys = (
    base
    .select("city", "id")
    .intersect(
        df_suppliers
        .filter(F.col("country").isin(["JP", "TH", "UK"]))
        .select("city", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("city").isNotNull())
    .select(
        "supplier_name",
        "country",
        "city",
        "supplier_name_slug",
        "city_filled",
        F.lower("supplier_name").alias("supplier_name_lower"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

base_suppliers = (
    filtered
    .filter(F.col("country").isin(["US", "TH", "JP"]))
    .select(
        "supplier_name",
        "city",
        "supplier_name_slug",
        "city_filled",
        "supplier_name_lower",
        "resolved_city",
        F.length("city").alias("city_length"),
    )
)

result = (
    base_suppliers
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("city_length").alias("min_city_length"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .filter(F.col("chiang_mai_count") >= 1)
    .orderBy(F.col("city").desc_nulls_last())
)

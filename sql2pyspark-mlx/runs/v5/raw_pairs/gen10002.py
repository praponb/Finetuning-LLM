from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.lower("city").alias("city_lower"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

filtered_suppliers = (
    filtered
    .filter(F.col("city").isNotNull())
    .select("id", "city", "city_lower", F.length("country").alias("country_length"))
)

cleaned = (
    filtered_suppliers
    .filter(F.col("country_length") > 31)
    .withColumn("country_length_whole", F.ceil("country_length"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.size(F.col("c.tags")) >= 1), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost").isNotNull()), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "city",
        "country_length",
        "country_length_whole",
        F.when(F.col("city").rlike("^(Singapore|Chiang Mai)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|New York)$"), "GROUP_B").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("city").asc_nulls_last())
)

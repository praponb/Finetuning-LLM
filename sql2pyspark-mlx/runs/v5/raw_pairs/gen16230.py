from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.length("supplier_name") >= 6)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("country").isin(["UK", "TH"]), "Tier 1").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") > 310), "left_semi")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") >= 780), "left_anti")
)

cleaned = (
    not_flagged_suppliers
    .filter(F.col("country") == "DE")
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
    .withColumn("country_length", F.length("country"))
)

kept_keys = (
    cleaned
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("city") == "Berlin")
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("city", "country")
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.percentile_approx("country_length", [0.25, 0.5, 0.75]).alias("country_length_quartiles"),
        F.percentile_approx("country_length", [0.25, 0.5, 0.75]).alias("country_length_quartiles_2"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .filter(F.col("jp_rows") > 1)
    .orderBy(F.col("distinct_cities").desc_nulls_first())
)

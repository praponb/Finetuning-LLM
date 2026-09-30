from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)

cleaned = (
    filtered
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.trim("supplier_name").alias("clean_supplier_name_2"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") != "Fashion"), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (~F.col("x.brand").isin(["Initech", "Stark", "Acme"])), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand") == "Umbrella"), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "clean_supplier_name",
        "city_filled",
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)

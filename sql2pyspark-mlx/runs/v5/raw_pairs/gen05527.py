from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("id", "supplier_name", "country", F.trim("country").alias("clean_country"))
)

matched_keys = (
    filtered
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") <= 3.3)
        .select("country")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand") == "Acme"), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "*",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
        F.upper("country").alias("country_upper"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "supplier_name",
        "is_th",
        F.when(F.col("supplier_name").rlike("^(Global Supply|Pacific Wholesale)$"), "GROUP_B").otherwise("GROUP_A").alias("supplier_name_zone"),
    )
    .orderBy(F.col("id").desc(), F.col("is_th").desc())
)

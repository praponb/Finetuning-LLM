from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.upper(F.trim("supplier_name")) == "TOKYO TRADERS"))
    .select("*", F.round("rating", 1).alias("rating_rounded"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty").between(137, 224)), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("city")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "London")
        .select("city")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

cleaned_suppliers = (
    verified
    .filter(F.col("supplier_name").like("Gl%"))
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_rounded",
        F.concat("country", F.lit(" - "), "supplier_name").alias("country_supplier_name"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

cleaned_suppliers_2 = (
    cleaned_suppliers
    .filter(F.col("supplier_name").like("%Parts%"))
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_rounded",
        "country_supplier_name",
        "resolved_city",
        (F.col("rating_rounded") + F.col("rating")).alias("rating_rounded_plus_rating"),
        F.round((F.col("rating_rounded") * 100), 2).alias("adjusted_rating_rounded"),
    )
)

result = (
    cleaned_suppliers_2
    .select(
        "id",
        "rating",
        "rating_rounded",
        "rating_rounded_plus_rating",
        "adjusted_rating_rounded",
    )
    .orderBy(F.col("adjusted_rating_rounded").desc())
)

from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "US", "UK"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("rating") >= 4.4, "High").when(F.col("rating") >= 2.1, "Medium").otherwise("Low").alias("rating_level"),
    )
)

cleaned = (
    base
    .filter(F.col("city").isNotNull())
    .select("*", F.initcap(F.trim("country")).alias("country_title"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.category").isin(["Electronics", "Sports", "Books"])), "left_anti")
)

result = (
    not_flagged
    .select("id", "supplier_name", "country", "city", "rating_level")
    .orderBy("id", F.col("city").desc())
)

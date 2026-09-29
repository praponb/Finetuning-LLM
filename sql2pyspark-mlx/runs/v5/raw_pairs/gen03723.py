from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & ~F.col("supplier_name").like("S%"))
    .withColumn("rating_rounded", F.round("rating", 1))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

with_latest = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        "s.city_clean",
        F.col("c.category").alias("product_category"),
        F.col("c.cost"),
    )
)

valid = (
    latest_match
    .filter(F.col("product_category").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("product_category")).alias("product_category_title"),
        F.length("city").alias("city_length"),
        F.coalesce("product_category", F.lit("DIRECT")).alias("resolved_product_category"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("city_length") >= 65, "LARGE").otherwise("SMALL").alias("city_length_tier"),
    )
    .agg(
        F.percentile_approx("city_length", 0.5).alias("city_length_p50"),
        F.percentile_approx("city_length", [0.25, 0.5, 0.75]).alias("city_length_quartiles"),
        F.percentile_approx("city_length", [0.25, 0.5, 0.75]).alias("city_length_quartiles_2"),
        F.sum(F.when(F.col("product_category") == "Books", F.col("cost"))).alias("books_total_cost"),
    )
    .orderBy(F.col("city_length_p50").desc())
)

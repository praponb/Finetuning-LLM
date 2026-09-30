from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Chiang Mai"))) & F.col("city").isin(["Chiang Mai", "Tokyo", "Singapore"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

enriched = (
    cleaned
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "city",
        "rating",
        "country_title",
        F.count("rating").over(w).alias("last7_count_rating"),
        F.min("rating").over(w).alias("last7_min_rating"),
        F.max("rating").over(w).alias("last7_max_rating"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
    )
)

filtered = (
    enriched
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "city",
        "rating",
        "country_title",
        "last7_min_rating",
        "last7_max_rating",
        "prev_rating",
        (F.col("rating") + F.col("last7_min_rating")).alias("rating_plus_last7_min_rating"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        "s.country_title",
        "s.last7_max_rating",
        "s.prev_rating",
        "s.rating_plus_last7_min_rating",
        "s.city_filled",
        F.col("c.brand").alias("product_brand"),
        F.col("c.cost"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNotNull()), "left_semi")
)

result = (
    not_flagged
    .groupBy("city")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("cost"))).alias("chiang_mai_total_cost"),
    )
)

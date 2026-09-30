from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.regexp_replace("supplier_name", " ", "").alias("supplier_name_slug"))
)

cleaned = (
    base
    .filter(F.col("city").isNotNull() & (F.lower(F.trim("supplier_name")) == "lion city imports"))
    .select(
        "*",
        F.substring("city", 1, 2).alias("city_prefix"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") == "Grocery"), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"))
)

result = (
    with_latest
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.8, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        "country",
    )
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating_rounded_2_whole").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.round("rating", 0).alias("rating_rounded"),
        F.round("rating", 1).alias("rating_rounded_2"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("city").isNotNull() & (F.col("rating") < 3.5))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "rating_rounded",
        "rating_rounded_2",
        "city_filled",
        F.floor("rating_rounded_2").alias("rating_rounded_2_whole"),
    )
)

valid = (
    base
    .filter(F.col("city").isNotNull() & (F.col("rating_rounded_2") <= 38))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating_rounded_2",
        "city_filled",
        "rating_rounded_2_whole",
        F.when(F.col("rating_rounded") >= 63, "High").when(F.col("rating_rounded") >= 32, "Medium").otherwise("Low").alias("rating_rounded_band"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "country", "city_filled", "city_clean")
)

latest_match = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.product_name").isNull()), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "country",
        "city_filled",
        "stock_qty",
        F.when(F.col("country").rlike("^(SG|JP)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)

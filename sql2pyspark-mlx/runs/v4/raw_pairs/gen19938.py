from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Parts%"))
    .select(
        "*",
        F.col("rating").cast("int").alias("rating_int"),
        F.regexp_replace("supplier_name", " ", "-").alias("supplier_name_slug"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    latest
    .select("id", "country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") < 3.6)
        .select("id", "country")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("city_supplier_name", F.concat("city", F.lit(" "), "supplier_name"))
    .withColumn("rating_int_per_rating", F.round((F.col("rating_int") / F.col("rating")), 2))
)

filtered = (
    cleaned
    .filter(F.col("rating").between(2.3, 4.2))
    .select("*", F.round((F.col("rn") * 0.85), 2).alias("rn_adjusted"))
)

filtered_suppliers = (
    filtered
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "*",
        (F.col("rn_adjusted") - F.col("rating")).alias("rn_adjusted_minus_rating"),
    )
)

result = (
    filtered_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "rating_int",
        "supplier_name_slug",
        "resolved_city",
        "rn",
        "city_supplier_name",
        "rating_int_per_rating",
    )
    .orderBy(F.col("id").desc(), "rn")
)

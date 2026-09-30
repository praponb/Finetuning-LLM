from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w_ordered = Window.partitionBy("country").orderBy(F.col("country_length").asc_nulls_last())
w3 = Window.partitionBy("country").orderBy(F.col("country_length_percent_rank").desc_nulls_first())
w4 = Window.partitionBy("country").orderBy(F.col("country_length").desc())

cleaned = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 3))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

filtered = (
    cleaned
    .filter(F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "supplier_name_prefix",
        "city_clean",
        F.length("country").alias("country_length"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.upper(F.col("c.product_name")).like("%ESK%")), "left_anti")
)

kept_keys = (
    eligible
    .select("id", "country")
    .exceptAll(
        df_suppliers
        .filter(F.col("country").isin(["US", "DE"]))
        .select("id", "country")
    )
)

verified = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

filtered_suppliers = (
    verified
    .filter(F.col("country_length") != 44)
    .select(
        "*",
        F.upper("supplier_name").alias("supplier_name_upper"),
        F.least("rating", "country_length").alias("least_rating_country_length"),
    )
)

scored = (
    filtered_suppliers
    .filter(F.col("least_rating_country_length") >= 40)
    .select(
        "*",
        F.min("least_rating_country_length").over(w).alias("country_min_least_rating_country_length"),
        F.percent_rank().over(w_ordered).alias("country_length_percent_rank"),
    )
)

metrics = (
    scored
    .select(
        "rating",
        "supplier_name_prefix",
        "country_min_least_rating_country_length",
        "country_length_percent_rank",
        F.percent_rank().over(w3).alias("country_length_percent_rank_percent_rank"),
        F.rank().over(w4).alias("country_length_rank"),
    )
)

result = (
    metrics
    .select(
        "supplier_name_prefix",
        "country_min_least_rating_country_length",
        "country_length_percent_rank",
        "country_length_percent_rank_percent_rank",
        "country_length_rank",
    )
    .orderBy(
        F.col("country_length_percent_rank_percent_rank").desc(),
        "country_min_least_rating_country_length",
    )
)

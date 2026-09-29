from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country")
w2 = Window.orderBy(F.col("rating").asc_nulls_first())
w_ordered = Window.partitionBy("city").orderBy(F.col("rating_rank").desc_nulls_last())

kept_keys = (
    df_suppliers
    .select("city")
    .subtract(
        df_suppliers
        .filter(~F.col("country").isin(["TH", "UK"]))
        .select("city")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("city") != "New York")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.length("country").alias("country_length"),
    )
)

filtered = (
    valid
    .filter(F.col("city").isNotNull() & F.col("country").isin(["US", "DE"]))
    .withColumn("country_city", F.concat("country", F.lit(" - "), "city"))
    .withColumn("city_clean", F.coalesce("city", F.lit("N/A")))
)

metrics = (
    filtered
    .filter(F.col("country_length") < 26)
    .select(
        "id",
        "city",
        "rating",
        "country_city",
        F.sum("country_length").over(w_hist).alias("country_sum_country_length"),
        F.rank().over(w2).alias("rating_rank"),
    )
)

matched_keys = (
    metrics
    .select("city")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "New York")
        .select("city")
    )
)

reconciled_suppliers = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled_suppliers
    .select(
        "*",
        F.rank().over(w_ordered).alias("rating_rank_rank"),
        F.dense_rank().over(w_ordered).alias("rating_rank_dense_rank"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy("row_num", "id")

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") == "Sports"), "left_semi")
)

latest = (
    kept
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

deduped = (
    with_last
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    deduped
    .filter(F.col("city").isNotNull() & (F.col("rating") >= 2.9))
    .select(
        "*",
        F.concat("country", F.lit("/"), "city").alias("country_city"),
        F.trim("city").alias("clean_city"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

base = (
    filtered
    .filter(F.col("rating").isNotNull())
    .withColumn("row_num_minus_rating", (F.col("row_num") - F.col("rating")))
    .withColumn("city_group", F.when(F.col("city").isin(["Bangkok", "Berlin"]), "Group A").otherwise("Other"))
    .withColumn("city_clean_2", F.coalesce("city", F.lit("OTHER")))
)

result = (
    base
    .groupBy("country")
    .agg(
        F.min("rating").alias("min_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.percentile_approx("row_num_minus_rating", 0.95).alias("row_num_minus_rating_p95"),
    )
)

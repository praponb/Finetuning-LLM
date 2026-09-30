from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("country").orderBy("rating_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy("country_stddev_samp_rating_int", "id").rowsBetween(-5, -1)

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "city", "supplier_name").alias("city_supplier_name_key"),
        F.concat("supplier_name", F.lit(" - "), "city").alias("supplier_name_city"),
    )
)

enriched = (
    cleaned
    .filter(F.col("supplier_name").isNull())
    .withColumn("rating_cume_dist", F.cume_dist().over(w))
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w2))
)

base = (
    enriched
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "rating",
        "supplier_name_city",
        "rating_cume_dist",
        "last_known_rating",
        F.substring("country", 1, 4).alias("country_prefix"),
    )
)

base_suppliers = (
    base
    .filter(F.col("country").isin(["UK", "DE", "SG"]))
    .select("*", F.col("rating").cast("int").alias("rating_int"))
)

scored = (
    base_suppliers
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w3)), 4))
    .withColumn("country_stddev_samp_rating_int", F.stddev_samp("rating_int").over(w3))
)

metrics = (
    scored
    .select(
        "*",
        F.last("country_stddev_samp_rating_int", ignorenulls=True).over(w4).alias("last_known_country_stddev_samp_rating_int"),
        F.sum("country_stddev_samp_rating_int").over(w3).alias("country_sum_country_stddev_samp_rating_int"),
        F.avg("last_known_rating").over(w_grp).alias("last_known_rating_avg_5_prior"),
    )
)

cleaned_suppliers = (
    metrics
    .filter(F.col("rating_share").isNull())
    .withColumn("rating_int_2", F.col("rating").cast("int"))
)

result = (
    cleaned_suppliers
    .groupBy("country")
    .agg(
        F.max(F.when(F.col("country") == "DE", F.col("rating_int_2"))).alias("de_max_rating_int_2"),
        F.min("rating_int").alias("min_rating_int"),
        F.percentile_approx("rating_int", 0.95).alias("rating_int_p95"),
        F.sum(F.when(F.col("country") == "UK", F.col("rating_cume_dist")).otherwise(0)).alias("uk_rating_cume_dist"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .orderBy("min_rating_int")
)

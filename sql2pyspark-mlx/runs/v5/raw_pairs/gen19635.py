from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "country").orderBy("running_avg_rating", "id")

valid = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat("supplier_name", F.lit(" "), "city").alias("supplier_name_city"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.category") == "Home"), "left_anti")
)

metrics = (
    kept
    .withColumn("running_count_rating", F.count("rating").over(w))
    .withColumn("running_max_rating", F.max("rating").over(w))
    .withColumn("running_avg_rating", F.avg("rating").over(w))
    .withColumn("running_sum_rating", F.sum("rating").over(w))
    .withColumn("city_stddev_samp_rating", F.stddev_samp("rating").over(w2))
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "supplier_name_city", "city_stddev_samp_rating")
)

eligible = (
    deduped
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost").between(430, 510)), "left_semi")
)

result = (
    eligible
    .groupBy("city")
    .agg(
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.percentile_approx("city_stddev_samp_rating", [0.25, 0.5, 0.75]).alias("city_stddev_samp_rating_quartiles"),
        F.min("city_stddev_samp_rating").alias("min_city_stddev_samp_rating"),
        F.percentile_approx("city_stddev_samp_rating", 0.95).alias("city_stddev_samp_rating_p95"),
    )
    .orderBy(F.col("min_city_stddev_samp_rating").desc_nulls_first())
)

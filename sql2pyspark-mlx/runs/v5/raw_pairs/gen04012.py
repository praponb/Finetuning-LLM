from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city", "country")
w_ordered = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.orderBy("rating", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("product_category").orderBy(F.col("country_sum_rating").desc_nulls_first(), "id")

valid = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Singapore"))) & F.col("supplier_name").like("%Part%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

with_windows = (
    valid
    .withColumn("country_sum_rating", F.sum("rating").over(w))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
    .withColumn("rating_bucket", F.ntile(10).over(w_ordered))
    .withColumn("rating_avg_2_prior", F.avg("rating").over(w4))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        "s.clean_supplier_name",
        "s.city_clean",
        "s.country_sum_rating",
        F.col("c.category").alias("product_category"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

deduped = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("MONTH", "product_launch_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("country_sum_rating")).otherwise(0)).alias("berlin_country_sum_rating"),
        F.sum("rating").alias("total_rating"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)

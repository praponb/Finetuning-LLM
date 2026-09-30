from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w_grp = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w4 = Window.partitionBy("country").orderBy(F.col("rating_bucket").asc_nulls_first())

valid = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Chiang Mai"]))
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", "-"))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .withColumn("rating_bucket", F.ntile(4).over(w))
    .withColumn("rating_bucket_2", F.ntile(4).over(w_grp))
    .withColumn("rating_percent_rank", F.percent_rank().over(w3))
)

kept = (
    metrics
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") == "Umbrella"), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand").isNull()), "left_anti")
)

latest = (
    eligible
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .cube(F.when(F.col("rating") >= 3.6, "HIGH").otherwise("LOW").alias("rating_tier"))
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.sum(F.when(F.col("country") == "US", F.col("rating_bucket")).otherwise(0)).alias("us_rating_bucket"),
        F.sum(F.when(F.col("country") == "US", F.col("rating"))).alias("us_total_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("rating_tier").desc_nulls_first())
)

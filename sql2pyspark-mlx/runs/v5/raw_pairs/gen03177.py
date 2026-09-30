from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("running_max_rating", "id")
w3 = Window.orderBy("running_sum_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("country").orderBy("running_max_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy(F.col("running_count_rating_share").asc_nulls_first())
w7 = Window.partitionBy("running_count_rating_tier").orderBy(F.col("berlin_count").desc())

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").isNotNull()), "left_anti")
)

metrics = (
    kept
    .filter(F.length("supplier_name") >= 10)
    .select(
        "*",
        F.count("rating").over(w).alias("running_count_rating"),
        F.max("rating").over(w).alias("running_max_rating"),
        F.sum("rating").over(w).alias("running_sum_rating"),
    )
)

enriched = (
    metrics
    .withColumn("running_max_rating_bucket", F.ntile(4).over(w2))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w3))
    .withColumn("running_count_rating_share", F.round((F.col("running_count_rating") / F.sum("running_count_rating").over(w4)), 4))
    .withColumn("first_known_running_max_rating", F.first("running_max_rating", ignorenulls=True).over(w5))
)

ranked = (
    enriched
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") == "Globex"), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("running_count_rating") >= 17, "PREMIUM").otherwise("STANDARD").alias("running_count_rating_tier"),
        "city",
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("num_cities") > 1)
    .withColumn("berlin_count_percent_rank", F.percent_rank().over(w7))
    .orderBy(F.col("num_cities").desc_nulls_last())
)

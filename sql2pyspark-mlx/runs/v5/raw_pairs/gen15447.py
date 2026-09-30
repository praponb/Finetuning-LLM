from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("city", "product_brand").orderBy("rating_whole", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("id", "city", "rating", F.trim("country").alias("clean_country"))
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered_suppliers = (
    deduped
    .filter(F.col("city").isin(["Singapore", "Berlin"]))
    .withColumn("rating_whole", F.ceil("rating"))
)

with_latest = (
    filtered_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.price").alias("product_price"),
        F.col("c.brand").alias("product_brand"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category").isin(["Books", "Electronics", "Fashion"])), "left_semi")
)

scored = (
    kept
    .select(
        "id",
        "rating",
        "product_price",
        F.sum("product_price").over(w_running).alias("last3_sum_product_price"),
        F.max("product_price").over(w_running).alias("last3_max_product_price"),
        F.count("product_price").over(w_running).alias("last3_count_product_price"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("last3_count_product_price") >= 13, "HIGH").otherwise("LOW").alias("last3_count_product_price_tier"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
    )
    .filter(F.col("avg_rating") > 0)
    .orderBy(F.col("avg_rating").asc_nulls_last())
)

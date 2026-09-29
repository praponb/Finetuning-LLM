from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("rating_share", "id")
w4 = Window.partitionBy("country").orderBy("rating_share", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city", "country").orderBy("rating", "id")
w_hist = Window.partitionBy("country").orderBy(F.col("rating_bucket").desc(), "id")
w8 = Window.partitionBy("country").orderBy("rating_bucket", "id")
w9 = Window.partitionBy("city", "country")
w10 = Window.partitionBy("country").orderBy("rating")

metrics = (
    df_suppliers
    .withColumn("rating_cume_dist", F.cume_dist().over(w))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
)

metrics_suppliers = (
    metrics
    .select(
        "*",
        F.lead("rating", 1).over(w3).alias("next_rating"),
        F.avg("rating_cume_dist").over(w4).alias("rating_cume_dist_avg_2"),
        F.sum("rating_cume_dist").over(w5).alias("cumulative_rating_cume_dist"),
        F.ntile(3).over(w6).alias("rating_bucket"),
        F.max("rating_share").over(w2).alias("city_max_rating_share"),
    )
)

scored = (
    metrics_suppliers
    .withColumn("rating_bucket_bucket", F.ntile(3).over(w_hist))
    .withColumn("rating_bucket_bucket_2", F.ntile(3).over(w8))
)

matched_keys = (
    scored
    .select("id", "city")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("Tok%"))
        .select("id", "city")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored_suppliers = (
    kept_rows
    .filter(F.col("city_max_rating_share") != 0)
    .withColumn("city_min_rating_share", F.min("rating_share").over(w9))
    .withColumn("rating_cume_dist_2", F.cume_dist().over(w10))
)

filtered = (
    scored_suppliers
    .filter(F.col("country") == "SG")
    .select(
        "*",
        (F.col("rating_bucket_bucket_2") - F.col("city_max_rating_share")).alias("rating_bucket_bucket_2_minus_city_max_rating_share"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").like("%Wate%")), "left_anti")
)

result = (
    eligible
    .select(
        "city",
        F.col("rating_cume_dist_2").alias("RATING_CUME_DIST_2_VAL"),
        F.col("rating_cume_dist").alias("RATING_CUME_DIST_VAL"),
        F.col("rating").alias("RATING"),
    )
    .unpivot(
        ids=["city"],
        values=["RATING_CUME_DIST_2_VAL", "RATING_CUME_DIST_VAL", "RATING"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

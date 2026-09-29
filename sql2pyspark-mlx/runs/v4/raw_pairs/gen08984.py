from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating_int").desc(), "id")
w3 = Window.partitionBy("city").orderBy("rating", "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.col("rating").cast("int").alias("rating_int"),
        F.regexp_replace("supplier_name", " ", "-").alias("supplier_name_slug"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.lead("rating_int", 1).over(w).alias("next_rating_int"),
        F.ntile(10).over(w2).alias("rating_int_bucket"),
        F.ntile(10).over(w3).alias("rating_bucket"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.upper(F.trim(F.col("ch.product_name"))) == "OFFICE CHAIR"), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("rating_int") >= 86, "HIGH").otherwise("LOW").alias("rating_int_tier"),
        "city",
    )
    .agg(
        F.sum("rating_bucket").alias("total_rating_bucket"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("rating_int_bucket", [0.25, 0.5, 0.75]).alias("rating_int_bucket_quartiles"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("rating_per_tokyo"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
    )
    .filter(F.col("rating_per_tokyo") >= 0)
)

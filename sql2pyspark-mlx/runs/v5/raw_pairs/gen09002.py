from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "London", "Singapore"]) & (F.col("city").eqNullSafe(F.lit("London"))))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.category").alias("product_category"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

kept = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty").between(190, 272)), "left_semi")
)

kept_keys = (
    kept
    .select("city", "id")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "New York")
        .select("city", "id")
    )
)

verified = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("supplier_name_length") >= 69, "LARGE").otherwise("SMALL").alias("supplier_name_length_tier"),
        F.when(F.col("rating") >= 4.5, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("supplier_name_length"), 2).alias("avg_supplier_name_length"),
        F.max(F.when(F.col("product_category") == "Books", F.col("rating"))).alias("books_max_rating"),
        F.sort_array(F.collect_set("product_category")).alias("product_category_list"),
        F.percentile_approx("product_stock_qty", 0.5).alias("product_stock_qty_p50"),
        F.sum("rating").alias("total_rating"),
    )
)

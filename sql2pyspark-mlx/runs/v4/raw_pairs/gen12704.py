from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("product_price", "id")
w4 = Window.orderBy("rating", "id")
w_running = Window.partitionBy("city", "country").orderBy("rating", "id")
w_part = Window.orderBy(F.col("product_stock_qty_avg_5").asc_nulls_first())
w7 = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_last())
w8 = Window.partitionBy("city").orderBy(F.col("rating_bucket").asc_nulls_first(), "id")

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "bestseller")), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("rating") < 3.6)
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.concat("country", F.lit(", "), "city").alias("country_city"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.stock_qty").alias("product_stock_qty"),
        F.col("c.price").alias("product_price"),
    )
)

metrics = (
    latest_match
    .select(
        "*",
        F.avg("product_stock_qty").over(w_hist).alias("product_stock_qty_avg_5"),
        F.lead("rating", 2, 0).over(w3).alias("next_rating"),
    )
)

metrics_suppliers = (
    metrics
    .select(
        "*",
        F.lead("product_stock_qty", 1).over(w4).alias("next_product_stock_qty"),
        F.ntile(3).over(w_running).alias("rating_bucket"),
        F.cume_dist().over(w_part).alias("product_stock_qty_avg_5_cume_dist"),
        F.dense_rank().over(w7).alias("rating_dense_rank"),
    )
)

deduped = (
    metrics_suppliers
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    deduped
    .select("country", "id")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["DE", "UK"]))
        .select("country", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "country",
        "city",
        "rating",
        "country_city",
        "resolved_city",
        "next_product_stock_qty",
        "rating_bucket",
        "product_stock_qty_avg_5_cume_dist",
        "dedup_rank",
    )
    .orderBy(F.col("city").desc())
)

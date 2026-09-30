from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("cumulative_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category", "city").orderBy(F.col("running_max_rating").desc_nulls_first())
w5 = Window.partitionBy("category", "supplier_id").orderBy("stock_qty", "id")
w6 = Window.partitionBy("supplier_id").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("supplier_id").orderBy(F.col("rating").desc())

with_dims = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id").eqNullSafe(F.col("s.id")), "inner")
    .filter(F.col("s.supplier_name").like("%Trade%"))
    .select("a.*", F.col("s.rating"), F.col("s.supplier_name"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "rating",
        "supplier_name",
        F.max("rating").over(w).alias("running_max_rating"),
        F.min("rating").over(w).alias("running_min_rating"),
        F.avg("rating").over(w).alias("running_avg_rating"),
        F.sum("rating").over(w).alias("running_sum_rating"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.store_id").isin([4, 5])), "left_semi")
)

enriched = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "inner")
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.stock_qty",
        "s.supplier_id",
        "s.rating",
        "s.running_max_rating",
        "s.running_avg_rating",
        "s.cumulative_rating",
        F.col("p2.city"),
        F.col("p2.supplier_name").alias("supplier_supplier_name"),
        F.col("p2.rating").alias("supplier_rating"),
    )
)

with_windows_products = (
    enriched
    .select(
        "id",
        "category",
        "stock_qty",
        "city",
        "supplier_supplier_name",
        "supplier_rating",
        F.sum("cumulative_rating").over(w3).alias("cumulative_cumulative_rating"),
        F.cume_dist().over(w4).alias("running_max_rating_cume_dist"),
        F.lead("running_avg_rating", 2).over(w5).alias("next_running_avg_rating"),
        F.last("stock_qty", ignorenulls=True).over(w6).alias("last_known_stock_qty"),
        F.percent_rank().over(w7).alias("rating_percent_rank"),
    )
)

result = (
    with_windows_products
    .groupBy(
        F.when(F.col("supplier_rating") >= 3.6, "LARGE").otherwise("SMALL").alias("supplier_rating_tier"),
        "city",
        F.when(F.col("stock_qty") >= 378, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.max("next_running_avg_rating").alias("max_next_running_avg_rating"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.sum(F.when(F.col("category") == "Home", F.col("rating_percent_rank")).otherwise(0)).alias("home_rating_percent_rank"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("home_rating_percent_rank") > 0)
)

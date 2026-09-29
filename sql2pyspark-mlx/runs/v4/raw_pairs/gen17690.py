from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id")
w2 = Window.partitionBy("category")
w_hist = Window.orderBy(F.col("price").asc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_last())
w5 = Window.orderBy(F.col("revenue").desc_nulls_last())

filtered = (
    df_store_sales
    .filter(F.col("revenue").between(1700, 3600))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

matched_keys = (
    filtered
    .select("id", "product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("revenue").isNull())
        .select("id", "product_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.price"), F.col("d.category"), F.col("d.launch_date"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.corr("revenue", "units").over(w_part).alias("corr_revenue_units"),
        F.corr("units", "revenue").over(w_part).alias("corr_units_revenue"),
    )
)

metrics = (
    with_windows
    .withColumn("category_count_corr_revenue_units", F.count("corr_revenue_units").over(w2))
    .withColumn("price_rank", F.rank().over(w_hist))
    .withColumn("revenue_percent_rank", F.percent_rank().over(w4))
)

with_dims_store_sales = (
    metrics
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.launch_date").alias("product_launch_date"))
)

with_windows_store_sales = (
    with_dims_store_sales
    .withColumn("revenue_cume_dist", F.cume_dist().over(w5))
    .withColumn("product_id_max_category_count_corr_revenue_units", F.max("category_count_corr_revenue_units").over(w_part))
)

result = (
    with_windows_store_sales
    .groupBy(
        "category",
        F.when(F.col("revenue") >= 3100, "LARGE").otherwise("SMALL").alias("revenue_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum("revenue") / F.nullif(F.count(F.when(F.col("category") == "Books", 1)), F.lit(0)), 2).alias("revenue_per_books"),
        F.min("corr_revenue_units").alias("min_corr_revenue_units"),
    )
)

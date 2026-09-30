from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "country").orderBy(F.col("cost").desc())
w2 = Window.partitionBy("country", "supplier_id")

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-03-18"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.concat("category", F.lit(" "), "product_name").alias("category_product_name"),
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
    )
)

remaining_keys = (
    filtered
    .select("supplier_id", "brand")
    .exceptAll(
        df_products
        .filter(F.col("launch_date") > "2024-05-01")
        .select("supplier_id", "brand")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 2, 4, 6])), "left_anti")
)

joined = (
    eligible
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .filter(~F.col("b.country").isin(["UK", "US", "JP"]))
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.price",
        "src.cost",
        "src.stock_qty",
        "src.tags",
        "src.supplier_id",
        F.col("b.country"),
        F.col("b.supplier_name"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "supplier_id",
        "country",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.max("stock_qty").over(w2).alias("country_max_stock_qty"),
    )
)

result = (
    enriched
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
)

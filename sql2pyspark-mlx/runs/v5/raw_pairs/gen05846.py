from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc())
w2 = Window.orderBy(F.col("revenue_adjusted").desc_nulls_last())
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy("stock_qty")
w5 = Window.partitionBy("product_id").orderBy(F.col("product_stock_qty").asc_nulls_first())

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-07-27").cast("date")) & (F.col("revenue") >= 3400))
    .select("*", F.round((F.col("revenue") * 1.07), 2).alias("revenue_adjusted"))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.units",
        "a.revenue_adjusted",
        F.col("d.store_name"),
        F.col("d.region"),
    )
)

remaining_keys = (
    with_dims
    .select("product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 4]))
        .select("product_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("region").isNotNull() & (F.col("region").eqNullSafe(F.lit("North"))))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue_adjusted",
        "store_name",
        "region",
        F.greatest("units", "revenue_adjusted").alias("greatest_units_revenue_adjusted"),
        F.upper("store_name").alias("store_name_upper"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.stock_qty"))
)

with_dims_store_sales = (
    joined
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.brand") == "Umbrella")
    .select(
        "e.product_id",
        "e.sale_date",
        "e.revenue_adjusted",
        "e.greatest_units_revenue_adjusted",
        "e.store_name_upper",
        "e.stock_qty",
        F.col("d.cost"),
        F.col("d.stock_qty").alias("product_stock_qty"),
        F.col("d.price"),
    )
)

enriched = (
    with_dims_store_sales
    .select(
        "product_id",
        "sale_date",
        "store_name_upper",
        "cost",
        "price",
        F.cume_dist().over(w).alias("cost_cume_dist"),
        F.rank().over(w2).alias("revenue_adjusted_rank"),
        F.sum("greatest_units_revenue_adjusted").over(w3).alias("product_id_sum_greatest_units_revenue_adjusted"),
        F.dense_rank().over(w4).alias("stock_qty_dense_rank"),
        F.rank().over(w5).alias("product_stock_qty_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
        F.when(F.col("stock_qty_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("stock_qty_dense_rank_tier"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
    )
)

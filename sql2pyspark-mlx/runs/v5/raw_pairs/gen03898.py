from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("revenue_per_unit", "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id", "store_id").orderBy("price", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("opened_date", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("store_id").orderBy("revenue_per_unit")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-23").cast("date"))
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
)

with_dims = (
    valid
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.product_name"), F.col("d.price"), F.col("d.category"))
)

metrics = (
    with_dims
    .select(
        "id",
        "store_id",
        "revenue_per_unit",
        "product_name",
        "category",
        F.sum("revenue_per_unit").over(w).alias("cumulative_revenue_per_unit"),
        F.row_number().over(w2).alias("event_seq"),
        F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4).alias("revenue_share"),
        F.avg("price").over(w4).alias("price_avg_2"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.region") == "South")
    .select("s.*", F.col("d.region"))
)

with_dims_store_sales = (
    joined
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.revenue_per_unit",
        "e.product_name",
        "e.cumulative_revenue_per_unit",
        "e.revenue_share",
        F.col("b.opened_date"),
    )
)

metrics_store_sales = (
    with_dims_store_sales
    .select(
        "id",
        "store_id",
        "revenue_share",
        "opened_date",
        F.avg("cumulative_revenue_per_unit").over(w5).alias("cumulative_revenue_per_unit_avg_6_prior"),
        F.count("cumulative_revenue_per_unit").over(w3).alias("store_id_count_cumulative_revenue_per_unit"),
        F.rank().over(w6).alias("revenue_per_unit_rank"),
    )
)

with_dims_store_sales_2 = (
    metrics_store_sales
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.opened_date",
        "src.revenue_per_unit_rank",
        F.col("p.city"),
        F.col("p.sqft"),
        F.col("p.opened_date").alias("store_opened_date"),
    )
)

result = (
    with_dims_store_sales_2
    .rollup(
        F.when(F.col("sqft") >= 12977, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.avg("revenue_per_unit_rank").alias("avg_revenue_per_unit_rank"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
    .orderBy("sqft_tier")
)

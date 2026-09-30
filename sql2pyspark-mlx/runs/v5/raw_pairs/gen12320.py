from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy("units", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last())
w4 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units_bucket").desc_nulls_last())

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-04-29").cast("date")) & (F.col("units") != 22))
    .select("*", (F.col("units") + F.col("revenue")).alias("revenue_per_unit"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.lag("revenue", 1).over(w).alias("prev_revenue"),
        F.ntile(3).over(w2).alias("units_bucket"),
        F.percent_rank().over(w3).alias("revenue_percent_rank"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.sale_date",
        "e.units",
        "e.revenue",
        "e.revenue_per_unit",
        "e.units_bucket",
        F.col("p.cost"),
        F.col("p.category"),
        F.col("p.price"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("price") >= 1090, "HIGH").otherwise("LOW").alias("price_tier"),
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .orderBy(F.col("books_rows").asc_nulls_first())
)

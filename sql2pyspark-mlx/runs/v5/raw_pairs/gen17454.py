from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w6 = Window.partitionBy("brand").orderBy("cost_bucket", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("brand").orderBy("cost_bucket", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-08-31"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_join("tags", "|").alias("tags_text"),
        F.upper("brand").alias("brand_upper"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("price") > 1200)
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "brand_upper",
        F.row_number().over(w).alias("event_seq"),
        F.sum("stock_qty").over(w2).alias("cumulative_stock_qty"),
        F.sum("price").over(w3).alias("cumulative_price"),
        F.ntile(3).over(w4).alias("cost_bucket"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "launch_date",
        "brand_upper",
        "cumulative_stock_qty",
        "cumulative_price",
        "row_seq",
        F.avg("cumulative_stock_qty").over(w6).alias("cumulative_stock_qty_avg_5_prior"),
        F.avg("cumulative_stock_qty").over(w7).alias("cumulative_stock_qty_avg_2_prior"),
        F.avg("price").over(w8).alias("price_avg_6"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.revenue") > 3500), "left_anti")
)

filtered = (
    kept
    .filter(F.col("brand").isNotNull())
    .withColumn("least_cumulative_stock_qty_price_avg_6", F.least("cumulative_stock_qty", "price_avg_6"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("UNKNOWN")))
)

result = (
    filtered
    .withColumn("activity_month", F.date_trunc("month", "launch_date"))
    .rollup("activity_month")
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.percentile_approx("price_avg_6", [0.25, 0.5, 0.75]).alias("price_avg_6_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("acme_rows") > 1)
    .orderBy("acme_rows")
)

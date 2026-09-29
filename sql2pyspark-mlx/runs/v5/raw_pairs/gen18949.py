from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("brand").orderBy("price", "id")
w3 = Window.partitionBy("category").orderBy(F.col("store_sale_revenue").desc_nulls_first())
w4 = Window.partitionBy("brand", "category")
w5 = Window.partitionBy("category")
w6 = Window.partitionBy("supplier_id", "brand").orderBy("stock_qty", "id")
w7 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_latest = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"))
)

scored = (
    with_latest
    .select(
        "*",
        F.row_number().over(w_hist).alias("seq_no"),
        F.percent_rank().over(w3).alias("store_sale_revenue_percent_rank"),
        F.stddev_samp("store_sale_revenue").over(w4).alias("brand_stddev_samp_store_sale_revenue"),
        F.corr("stock_qty", "price").over(w5).alias("corr_stock_qty_price"),
    )
)

metrics = (
    scored
    .filter(F.col("product_name").isNotNull())
    .select(
        "*",
        F.lead("brand_stddev_samp_store_sale_revenue", 1, 0).over(w6).alias("next_brand_stddev_samp_store_sale_revenue"),
        F.last("store_sale_revenue", ignorenulls=True).over(w7).alias("last_known_store_sale_revenue"),
    )
)

cleaned = (
    metrics
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-02-28"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "store_sale_revenue",
        "store_sale_revenue_percent_rank",
        "brand_stddev_samp_store_sale_revenue",
        "corr_stock_qty_price",
        "next_brand_stddev_samp_store_sale_revenue",
        "last_known_store_sale_revenue",
        (F.col("store_sale_revenue") / F.nullif("corr_stock_qty_price", F.lit(0))).alias("store_sale_revenue_per_corr_stock_qty_price"),
    )
)

filtered = (
    cleaned
    .filter(F.col("brand").isNotNull() & (F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "store_sale_revenue_percent_rank",
        "brand_stddev_samp_store_sale_revenue",
        "corr_stock_qty_price",
        "next_brand_stddev_samp_store_sale_revenue",
        "last_known_store_sale_revenue",
        "store_sale_revenue_per_corr_stock_qty_price",
        F.ceil("store_sale_revenue").alias("store_sale_revenue_whole"),
    )
)

result = (
    filtered
    .withColumn("cost_tier", F.when(F.col("cost") >= 790, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("year", "launch_date"))
    .groupingSets(
        [["cost_tier", "category", "year_start"], ["cost_tier", "category"], ["cost_tier"], []],
        "cost_tier",
        "category",
        "year_start",
    )
    .agg(
        F.avg(F.when(F.col("brand") == "Globex", F.col("corr_stock_qty_price"))).alias("globex_avg_corr_stock_qty_price"),
        F.round(F.sum("brand_stddev_samp_store_sale_revenue") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("brand_stddev_samp_store_sale_revenue_per_umbrella"),
        F.count("*").alias("row_count"),
        F.percentile_approx("store_sale_revenue_per_corr_stock_qty_price", 0.95).alias("store_sale_revenue_per_corr_stock_qty_price_p95"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy("brand_stddev_samp_store_sale_revenue_per_umbrella")
)

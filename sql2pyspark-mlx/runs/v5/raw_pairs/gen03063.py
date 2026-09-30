from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id")
w4 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("cost_bucket", "id")
w_recent = Window.orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_products
    .filter(F.col("supplier_id").isin([2, 30, 31, 40]))
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
        F.ntile(5).over(w3).alias("cost_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.sum("cost_bucket").over(w4).alias("cumulative_cost_bucket"),
        F.lag("stock_qty", 1, 0).over(w5).alias("prev_stock_qty"),
        F.first("cost_bucket", ignorenulls=True).over(w_recent).alias("first_known_cost_bucket"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "brand")
    .intersectAll(
        df_products
        .filter(F.col("category").isin(["Grocery", "Electronics", "Books"]))
        .select("id", "brand")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.launch_date")), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("first_known_cost_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("first_known_cost_bucket_tier"),
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("brand") == "Initech", F.col("prev_cost"))).alias("initech_avg_prev_cost"),
    )
    .filter(F.col("initech_avg_prev_cost") < 1000000)
    .orderBy(F.col("event_count").desc())
)

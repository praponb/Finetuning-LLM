from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("stock_qty").desc(), "id")
w_hist_2 = Window.partitionBy("product_id").orderBy("quantity_avg_5_per_stock_qty", "id")

scored = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        F.avg("quantity").over(w).alias("quantity_avg_5"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.stock_qty"), F.col("p.cost"))
)

valid = (
    with_dims
    .filter((F.col("last_restock") >= F.lit("2023-07-03").cast("date")) & F.col("cumulative_quantity").between(535, 635))
    .select(
        "id",
        "product_id",
        "quantity_avg_5",
        "stock_qty",
        "cost",
        (F.col("quantity_avg_5") / F.nullif("stock_qty", F.lit(0))).alias("quantity_avg_5_per_stock_qty"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.avg("quantity_avg_5").over(w_hist).alias("product_id_avg_quantity_avg_5"),
        F.ntile(3).over(w4).alias("stock_qty_bucket"),
        F.lead("quantity_avg_5_per_stock_qty", 1).over(w_hist_2).alias("next_quantity_avg_5_per_stock_qty"),
    )
)

valid_inventory = (
    metrics
    .filter(F.col("quantity_avg_5_per_stock_qty") > 52)
    .select(
        "*",
        F.greatest("stock_qty_bucket", "quantity_avg_5").alias("greatest_stock_qty_bucket_quantity_avg_5"),
    )
)

result = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        F.percentile_approx("quantity_avg_5", [0.25, 0.5, 0.75]).alias("quantity_avg_5_quartiles"),
        F.count("*").alias("cnt"),
        F.percentile_approx("stock_qty", 0.95).alias("stock_qty_p95"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)

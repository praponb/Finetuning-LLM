from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("cost_bucket").asc_nulls_first(), "id")

cleaned = (
    df_order_items
    .filter(F.col("unit_price") > 150)
    .select(
        "id",
        "product_id",
        "unit_price",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.id", "a.product_id", F.col("d.price"), F.col("d.cost"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.min("cost").over(w_grp).alias("running_min_cost"),
        F.sum("cost").over(w_grp).alias("running_sum_cost"),
        F.count("cost").over(w_grp).alias("running_count_cost"),
        F.max("cost").over(w_grp).alias("running_max_cost"),
        F.ntile(3).over(w_hist).alias("cost_bucket"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_id",
        "price",
        "cost",
        "running_sum_cost",
        "running_max_cost",
        "cost_bucket",
    )
)

with_dims_order_items = (
    ranked
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.price",
        "src.cost",
        "src.running_sum_cost",
        "src.cost_bucket",
        F.col("p.stock_qty"),
    )
)

result = (
    with_dims_order_items
    .select("id", "price", "stock_qty")
    .orderBy(F.col("id").desc_nulls_first())
)

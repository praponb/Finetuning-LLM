from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("quantity", "id")
w_part = Window.partitionBy("supplier_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-02").cast("date"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.lower("product_name").alias("product_name_lower"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"), F.col("b.supplier_name"))
)

with_latest = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

with_windows = (
    with_latest
    .select(
        "*",
        F.ntile(3).over(w2).alias("quantity_bucket"),
        F.avg("cost").over(w_part).alias("cost_avg_5"),
    )
)

result = (
    with_windows
    .groupBy("brand")
    .agg(
        F.max(F.when(F.col("category") == "Home", F.col("cost_avg_5"))).alias("home_max_cost_avg_5"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("stock_qty_per_initech"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
    .filter(F.col("home_max_cost_avg_5") >= 0)
)

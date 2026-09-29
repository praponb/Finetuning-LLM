from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc())
w_ordered = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

with_dims = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.supplier_name"))
)

filtered = (
    with_dims
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-02-23"))) & ~F.col("supplier_name").like("P%"))
    .withColumn("launch_date_minus_43d", F.date_sub("launch_date", 43))
)

ranked = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.launch_date")), "left_semi")
)

metrics = (
    eligible
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("running_count_cost", F.count("cost").over(w_ordered))
    .withColumn("running_max_cost", F.max("cost").over(w_ordered))
    .withColumn("running_min_cost", F.min("cost").over(w_ordered))
)

with_latest = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.cost",
        "s.tags",
        "s.launch_date_minus_43d",
        "s.row_num",
        "s.running_count_cost",
        "s.running_max_cost",
        "s.running_min_cost",
        F.col("c.sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

with_latest_products = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.launch_date_minus_43d",
        "s.running_count_cost",
        "s.running_max_cost",
        "s.running_min_cost",
        "s.store_sale_revenue",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_latest_products
    .select(
        "id",
        "running_max_cost",
        "store_sale_revenue",
        "order_item_unit_price",
        "order_item_quantity",
    )
    .orderBy("id")
    .limit(20)
)

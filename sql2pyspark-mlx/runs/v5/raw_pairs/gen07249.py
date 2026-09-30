from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("supplier_id", "country").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country", "category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("cost", "id")
w7 = Window.orderBy(F.col("cost").desc())

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-05-15")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper("category").alias("category_upper"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.category_upper",
        F.col("c.quantity"),
    )
)

with_dims = (
    latest_match
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .filter(F.col("p.country") != "DE")
    .select(
        "a.id",
        "a.category",
        "a.cost",
        "a.stock_qty",
        "a.launch_date",
        "a.supplier_id",
        "a.quantity",
        F.col("p.country"),
    )
)

metrics = (
    with_dims
    .withColumn("running_quantity", F.sum("quantity").over(w3))
    .withColumn("first_known_stock_qty", F.first("stock_qty", ignorenulls=True).over(w_grp))
    .withColumn("quantity_avg_6", F.avg("quantity").over(w_running))
    .withColumn("next_stock_qty", F.lead("stock_qty", 1).over(w6))
    .withColumn("cost_cume_dist", F.cume_dist().over(w7))
)

enriched = (
    metrics
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.cost",
        "a.stock_qty",
        "a.launch_date",
        "a.supplier_id",
        "a.first_known_stock_qty",
        "a.quantity_avg_6",
        "a.next_stock_qty",
        "a.cost_cume_dist",
        F.col("p.country").alias("supplier_country"),
    )
)

result = (
    enriched
    .select("id", "first_known_stock_qty", "quantity_avg_6", "cost_cume_dist")
    .orderBy(F.col("id").desc_nulls_last(), "cost_cume_dist")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
        F.when(F.col("brand").isin(["Stark", "Initech"]), "Tier 1").otherwise("Other").alias("brand_group"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

base = (
    latest_match
    .filter(F.col("stock_qty") != 306)
    .withColumn("cost_times_stock_qty_minus_price", (F.col("cost") * F.col("stock_qty_minus_price")))
)

with_last = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.units").alias("store_sale_units"),
    )
)

result = (
    with_last
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "stock_qty",
        "tags",
        "supplier_id",
        "stock_qty_minus_price",
        "brand_group",
        "order_item_quantity",
        "store_sale_revenue",
        F.when(F.col("category").rlike("^(Fashion|Home)$"), "EXTENDED").when(F.col("category").rlike("^(Electronics|Sports)$"), "GROUP_B").otherwise("CORE").alias("category_zone"),
        F.col("avg_product_cost"),
    )
    .orderBy(F.col("store_sale_revenue").desc())
)

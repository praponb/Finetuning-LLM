from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.orderBy("cost", "id")
w3 = Window.partitionBy("category").orderBy("inventory_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id", "category").orderBy("stock_qty_per_cost", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("supplier_id", "category").orderBy("inventory_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_products
    .filter(F.col("launch_date") > F.lit("2024-04-11").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.round((F.col("stock_qty") / F.col("cost")), 2).alias("stock_qty_per_cost"),
        F.when(F.col("brand") == "Initech", 1).otherwise(0).alias("is_initech"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

with_latest_products = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

result = (
    with_latest_products
    .select(
        "*",
        F.lead("cost", 1).over(w2).alias("next_cost"),
        F.sum("is_initech").over(w3).alias("running_is_initech"),
        F.avg("inventory_quantity").over(w4).alias("inventory_quantity_avg_2"),
        F.sum("is_initech").over(w5).alias("cumulative_is_initech"),
    )
)

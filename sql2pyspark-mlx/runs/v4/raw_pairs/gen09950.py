from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock_plus_51d", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("store_id").orderBy(F.col("quantity_bucket").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("units").alias("max_store_sale_units"))

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-07-06").cast("date")) & F.col("last_restock").between("2023-08-24", "2025-07-01"))
    .withColumn("last_restock_plus_51d", F.date_add("last_restock", 51))
)

scored = (
    base
    .withColumn("quantity_bucket", F.ntile(4).over(w))
    .withColumn("quantity_avg_2_prior", F.avg("quantity").over(w2))
)

scored_inventory = (
    scored
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w3))
    .withColumn("quantity_bucket_bucket", F.ntile(10).over(w_ordered))
    .withColumn("cumulative_quantity_bucket", F.sum("quantity_bucket").over(w5))
)

with_dims = (
    scored_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"))
)

result = (
    with_dims
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_plus_51d",
        "quantity_bucket",
        "quantity_avg_3_prior",
        "quantity_bucket_bucket",
        F.col("max_store_sale_units"),
    )
    .orderBy(F.col("last_restock_plus_51d").desc())
)

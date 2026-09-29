from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

result = (
    with_last
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "cost",
        "tags",
        "supplier_id",
        "inventory_quantity",
        F.when(F.col("product_name").rlike("^(Office Chair|Desk Lamp)$"), "PRIORITY").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "GROUP_B").otherwise("CORE").alias("product_name_zone"),
        F.col("max_product_price"),
    )
    .orderBy(F.col("inventory_quantity").desc())
)

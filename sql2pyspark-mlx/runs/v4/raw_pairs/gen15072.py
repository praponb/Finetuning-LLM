from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

latest_match = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.units").alias("store_sale_units"))
)

result = (
    latest_match
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(wireless mouse|Desk Lamp)$"), "CORE").when(F.col("product_name").rlike("^(Laptop Pro 14|USB-C Hub)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("product_name_zone"),
        F.col("sum_product_stock_qty"),
    )
    .orderBy("price")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-08-10")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        (F.col("price") - F.col("stock_qty")).alias("price_minus_stock_qty"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.supplier_id",
        F.col("c.units").alias("store_sale_units"),
    )
)

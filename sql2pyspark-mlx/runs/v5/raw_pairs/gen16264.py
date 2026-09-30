from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date_period").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("launch_date") >= F.lit("2023-08-02").cast("date")))
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
        F.when(F.col("price") >= 1070, "High").when(F.col("price") >= 590, "Medium").otherwise("Low").alias("price_tier"),
        F.date_trunc("year", "launch_date").alias("launch_date_period"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_last = (
    latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.price",
        "s.cost",
        "s.launch_date",
        "s.tags",
        "s.price_tier",
        "s.launch_date_period",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity").between(4, 8)), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "cost",
        "tags",
        "price_tier",
        "launch_date_period",
        "store_sale_sale_date",
        "revenue",
    )
    .orderBy(F.col("launch_date_period").desc())
)

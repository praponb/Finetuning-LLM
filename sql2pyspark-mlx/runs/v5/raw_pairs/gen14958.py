from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id")
w2 = Window.partitionBy("category")

metrics = (
    df_products
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("corr_price_stock_qty", F.corr("price", "stock_qty").over(w2))
)

result = (
    metrics
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
        F.when(F.col("product_name").rlike("^(Office Chair|USB-C Hub)$"), "OTHER_ZONE").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "EXTENDED").otherwise("CORE").alias("product_name_zone"),
    )
    .orderBy(F.col("price").asc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("cost").between(270, 490))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.length("brand").alias("brand_length"),
    )
)

result = (
    base
    .filter(F.col("category").isin(["Fashion", "Home", "Sports"]))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "tags",
        "supplier_id",
        F.count("brand_length").over(w).alias("last7_count_brand_length"),
        F.max("brand_length").over(w).alias("last7_max_brand_length"),
        F.avg("brand_length").over(w).alias("last7_avg_brand_length"),
    )
)

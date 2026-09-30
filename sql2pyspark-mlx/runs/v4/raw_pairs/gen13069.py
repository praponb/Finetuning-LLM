from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy("cost", "id")

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & F.array_contains("tags", "new"))
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
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.round((F.col("price") * 0.9), 2).alias("price_adjusted"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

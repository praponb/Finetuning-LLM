from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("price", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "launch_date",
        "tags",
        "supplier_id",
        F.max("price").over(w).alias("last7_max_price"),
        F.avg("price").over(w).alias("last7_avg_price"),
        F.sum("price").over(w).alias("last7_sum_price"),
        F.min("price").over(w).alias("last7_min_price"),
    )
)

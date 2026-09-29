from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").asc_nulls_last(), "id")

ranked = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "latest_rank",
    )
    .orderBy("cost")
)

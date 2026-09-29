from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost", "id")

deduped = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "latest_rank",
    )
    .orderBy(F.col("id").asc_nulls_last())
)

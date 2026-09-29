from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")

latest = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "tags",
        "supplier_id",
        "row_num",
    )
    .orderBy("row_num")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "product_id",
        F.first("product_id").over(w).alias("first_product_id"),
    )
    .orderBy("first_product_id")
)

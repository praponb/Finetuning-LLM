from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.row_number().over(w).alias("rn"),
    )
    .orderBy(F.col("id").desc(), "store_id")
)

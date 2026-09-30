from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc())
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2024-06-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "sale_date",
        F.dense_rank().over(w).alias("units_dense_rank"),
        (F.col("units") - F.lag("units", 1).over(w2)).alias("units_change"),
    )
)

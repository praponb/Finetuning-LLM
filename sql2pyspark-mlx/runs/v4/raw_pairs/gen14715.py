from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2025)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "product_id",
        "revenue",
        F.sum("units").over(w).alias("running_units"),
        F.dense_rank().over(w2).alias("revenue_dense_rank"),
    )
)

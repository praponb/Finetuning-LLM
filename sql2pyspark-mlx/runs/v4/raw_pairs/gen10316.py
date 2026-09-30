from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("units") > 22)
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        "units",
        F.row_number().over(w).alias("rn"),
        F.min("units").over(w2).alias("product_id_min_units"),
    )
    .orderBy("id")
)

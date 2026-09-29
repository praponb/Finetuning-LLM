from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 6]))
    .select(
        "id",
        "store_id",
        "units",
        "product_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .orderBy("pos")
)

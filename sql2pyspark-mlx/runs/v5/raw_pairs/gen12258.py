from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1600)
    .select(
        "id",
        "store_id",
        "revenue",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("rn"),
    )
)

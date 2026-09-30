from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        "sale_date",
        "units",
        F.dense_rank().over(w).alias("revenue_dense_rank"),
        F.lag("units", 1, 0).over(w2).alias("prev_units"),
    )
    .orderBy(F.col("revenue_dense_rank").desc(), "sale_date")
)

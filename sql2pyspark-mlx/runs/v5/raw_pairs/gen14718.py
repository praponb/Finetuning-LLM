from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 9]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "store_id",
        "revenue",
        F.round(F.avg("units").over(w), 2).alias("units_moving_avg_7"),
        F.dense_rank().over(w2).alias("revenue_dense_rank"),
    )
    .orderBy(F.col("units_moving_avg_7").desc())
)

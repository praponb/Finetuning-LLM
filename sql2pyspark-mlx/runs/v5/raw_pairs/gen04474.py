from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select(
        "*",
        F.min("revenue").over(w).alias("running_min_revenue"),
        F.max("revenue").over(w).alias("running_max_revenue"),
        F.avg("revenue").over(w).alias("running_avg_revenue"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

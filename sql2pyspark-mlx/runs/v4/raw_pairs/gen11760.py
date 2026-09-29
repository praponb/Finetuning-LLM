from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 6, 8]))
    .select("id", "revenue", F.row_number().over(w).alias("revenue_row"))
    .orderBy("id")
)

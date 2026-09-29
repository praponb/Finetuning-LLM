from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 4, 9]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "brand", "stock_qty")
    .orderBy("brand")
)

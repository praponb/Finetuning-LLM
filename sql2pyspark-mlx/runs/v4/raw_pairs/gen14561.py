from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .filter(F.col("units") < 37)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
)

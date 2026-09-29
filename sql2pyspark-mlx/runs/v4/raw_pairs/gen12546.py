from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("revenue") > 2800)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("units").desc())
)

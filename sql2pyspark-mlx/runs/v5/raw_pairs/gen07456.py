from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select("id", "product_id", "units", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "product_id", "units")
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
)

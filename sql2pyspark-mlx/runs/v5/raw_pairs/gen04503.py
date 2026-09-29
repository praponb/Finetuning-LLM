from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id")

result = (
    df_store_sales
    .select("id", "product_id", "units", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .orderBy(F.col("rn").desc())
)

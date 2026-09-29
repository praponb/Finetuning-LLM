from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .select("id", "brand", "cost", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
)

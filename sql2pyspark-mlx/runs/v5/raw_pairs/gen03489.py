from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .select("id", "category", "cost", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "category", "cost")
)

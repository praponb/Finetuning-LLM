from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.size("tags") > 2)
    .select("id", "category", "cost", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "category", "cost")
)

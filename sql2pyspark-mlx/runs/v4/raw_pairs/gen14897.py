from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .select("id", "city", "rating", "country", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
)

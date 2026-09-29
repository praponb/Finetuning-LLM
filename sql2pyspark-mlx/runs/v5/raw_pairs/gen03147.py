from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .select("id", "city", "rating", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "city", "rating")
)

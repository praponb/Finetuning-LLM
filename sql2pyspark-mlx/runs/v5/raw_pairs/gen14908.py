from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc())

result = (
    df_products
    .select("id", "category", "cost", "launch_date", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .select("id", "category", "launch_date", "price", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("rn").desc())
)

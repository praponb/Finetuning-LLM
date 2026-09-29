from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .select("id", "city", "rating", "country", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "city", "rating", "country")
)

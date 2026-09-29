from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "UK", "JP"]))
    .select("id", "city", "rating", "country", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

valid = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "Tokyo", "New York"]))
    .withColumn("country_title", F.initcap(F.trim("country")))
)

result = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

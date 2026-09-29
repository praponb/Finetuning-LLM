from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("sqft") != 12235)
    .select("id", "city", "opened_date", "sqft", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "city", "opened_date", "sqft")
    .orderBy(F.col("city").desc())
)

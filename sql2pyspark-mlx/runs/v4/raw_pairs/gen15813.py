from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")

result = (
    df_stores
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "city", "sqft")
    .orderBy(F.col("id").desc())
)

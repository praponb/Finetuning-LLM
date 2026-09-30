from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("sqft") >= 7891)
    .select("id", "region", "opened_date", "city", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "region", "opened_date", "city")
    .orderBy(F.col("id").desc())
)

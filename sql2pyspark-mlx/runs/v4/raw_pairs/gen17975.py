from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(~F.col("city").isin(["Tokyo", "London"]))
    .select("id", "region", "sqft", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("rn").desc())
)

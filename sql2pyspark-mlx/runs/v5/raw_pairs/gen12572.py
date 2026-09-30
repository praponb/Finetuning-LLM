from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Berlin", "Bangkok"]))
    .select("id", "region", "opened_date", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("opened_date").desc())
)

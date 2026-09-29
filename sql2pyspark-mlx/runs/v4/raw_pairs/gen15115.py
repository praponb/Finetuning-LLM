from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("opened_date") > "2024-02-01")
    .select("id", "region", "opened_date", "sqft", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "region", "opened_date", "sqft")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select("id", "store_id", "units", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "store_id", "units")
)

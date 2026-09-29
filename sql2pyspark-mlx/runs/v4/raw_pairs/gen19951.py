from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select("id", "store_id", "units", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy("store_id", F.col("units").desc())
)

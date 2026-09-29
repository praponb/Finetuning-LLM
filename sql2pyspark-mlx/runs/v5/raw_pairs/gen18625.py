from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .select("id", "region", "sqft", F.percent_rank().over(w).alias("sqft_pct_rank"))
)

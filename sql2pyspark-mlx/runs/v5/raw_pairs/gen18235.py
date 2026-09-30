from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select("id", "region", "sqft", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
)

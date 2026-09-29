from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .select("id", "region", "sqft", "city", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
)

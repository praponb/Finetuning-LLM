from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .filter(F.col("sqft") > 9312)
    .select("id", "city", "sqft", "store_name", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "city", "sqft", "store_name")
)

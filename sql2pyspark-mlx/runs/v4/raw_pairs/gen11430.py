from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select("id", "city", "region", "sqft", F.row_number().over(w).alias("sqft_row"))
    .orderBy(F.col("sqft_row").desc(), "region")
)

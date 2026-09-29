from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").asc_nulls_last(), "id")

latest = (
    df_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_name", "city", "opened_date")
)

result = latest.select("id", "store_name", "city", "opened_date")

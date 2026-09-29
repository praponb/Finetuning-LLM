from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "Chiang Mai", "New York"]))
    .select("id", "region", "opened_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("region").isin(["West", "Central"]))
    .select("id", "city", "opened_date", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "opened_date")
)

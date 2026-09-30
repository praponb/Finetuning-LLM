from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .filter(F.col("region") == "North")
    .select("id", "city", "opened_date", "store_name", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "city", "opened_date", "store_name")
    .orderBy("city", "id")
)

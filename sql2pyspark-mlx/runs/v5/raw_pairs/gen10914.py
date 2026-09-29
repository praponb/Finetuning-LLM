from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("city") == "Berlin")
    .select("id", "city", "opened_date", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "city", "opened_date")
)

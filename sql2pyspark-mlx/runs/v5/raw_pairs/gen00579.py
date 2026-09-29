from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date")

result = (
    df_stores
    .select("id", "region", "opened_date", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "region", "opened_date")
    .orderBy(F.col("opened_date").desc())
)

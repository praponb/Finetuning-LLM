from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("opened_date").desc())

latest = (
    df_stores
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select("id", "store_name", "region", "sqft", "rn")
    .orderBy(F.col("id").desc())
    .limit(20)
)

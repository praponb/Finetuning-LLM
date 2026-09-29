from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("is_chiang_mai").desc(), "id")

base = (
    df_stores
    .filter(F.col("sqft") < 7926)
    .withColumn("opened_date_dayofweek", F.dayofweek("opened_date"))
    .withColumn("is_chiang_mai", F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

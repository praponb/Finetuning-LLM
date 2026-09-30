from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age_num").desc_nulls_last(), "id")

base = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select("*", F.col("age").cast("double").alias("age_num"))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

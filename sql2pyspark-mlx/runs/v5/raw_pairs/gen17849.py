from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_last(), "id")

ranked = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .select("id", "store_name", "city", "region", "opened_date")
    .orderBy(F.col("city").desc())
)

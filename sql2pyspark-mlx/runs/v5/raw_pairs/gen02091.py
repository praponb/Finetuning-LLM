from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").asc_nulls_first(), "id")

latest = (
    df_stores
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select("id", "store_name", "region", "opened_date", "sqft", "dedup_rank")
    .orderBy(F.col("sqft").asc_nulls_last(), "opened_date")
)

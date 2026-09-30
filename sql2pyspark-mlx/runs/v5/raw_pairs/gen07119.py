from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("opened_date").desc(), "id")

ranked = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select("id", "store_name", "region", "opened_date", "sqft", "rank_in_group")
    .orderBy(F.col("opened_date").asc_nulls_last())
)

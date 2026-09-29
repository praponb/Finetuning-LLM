from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("opened_date").desc(), "id")

deduped = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = deduped.select("id", "store_name", "city", "region", "sqft", "rank_in_group")

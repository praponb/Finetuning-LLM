from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

deduped = (
    df_stores
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_name", "city")
)

result = (
    deduped
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("city") == "London").alias("london_events"),
    )
    .orderBy("london_events")
)

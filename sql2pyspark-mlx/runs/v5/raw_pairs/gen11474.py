from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_last())

deduped = (
    df_shipments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "carrier", "shipped_date", "delivered_date", "status")
)

result = (
    deduped
    .groupBy("carrier")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list_2"),
        F.count_if(F.col("status") == "LOST").alias("lost_events"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.size(F.collect_set("status")).alias("num_status_2"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

deduped = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "cost", "status")
)

result = (
    deduped
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost"))).alias("delivered_total_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("lost_count") >= 1)
)

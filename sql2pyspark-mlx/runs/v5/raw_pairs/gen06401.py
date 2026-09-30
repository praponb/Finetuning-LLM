from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("activity_month").orderBy(F.col("row_count").desc())

latest = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "carrier", "delivered_date")
)

result = (
    latest
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("activity_month"), "carrier")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("row_count_rank", F.rank().over(w2))
    .orderBy(F.col("row_count").desc_nulls_last())
)

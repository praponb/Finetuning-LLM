from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_first())

ranked = (
    df_orders
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.count("*").alias("cnt"),
        F.avg("discount").alias("avg_discount"),
    )
)

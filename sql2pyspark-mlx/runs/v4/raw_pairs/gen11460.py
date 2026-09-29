from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc_nulls_last(), "id")

latest = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("cost") >= 89, "LARGE").otherwise("SMALL").alias("cost_tier"),
        F.date_trunc("WEEK", "delivered_date").alias("week_start"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost"))).alias("flash_total_cost"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.stddev_samp("cost").alias("cost_stddev_2"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
    )
    .orderBy("flash_total_cost")
)

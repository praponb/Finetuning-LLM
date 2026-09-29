from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last(), "id")

latest = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .groupBy(
        F.date_trunc("YEAR", "delivered_date").alias("year_start"),
        "order_id",
        F.when(F.col("cost") >= 87, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.size(F.collect_set("carrier")).alias("num_carriers_2"),
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
        F.count_if(F.col("status") == "DELIVERED").alias("delivered_events"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("num_carriers_2") >= 1)
    .orderBy(F.col("num_carriers_2").asc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last(), "id")

latest = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("cost") >= 104, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("carrier") == "FedEx").alias("fedex_events"),
    )
    .filter(F.col("num_status") < 1)
)

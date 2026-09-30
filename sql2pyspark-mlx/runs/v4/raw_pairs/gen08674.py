from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc_nulls_first(), "id")

deduped = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupBy(
        "channel",
        "status",
        F.date_trunc("YEAR", "order_date").alias("year_start"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)

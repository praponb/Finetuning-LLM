from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("delivered_date").alias("last_delivered_date"),
    )
    .orderBy("last_delivered_date")
)

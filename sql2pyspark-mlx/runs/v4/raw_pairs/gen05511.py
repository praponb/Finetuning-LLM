from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-03-28")
    .groupBy(F.year("shipped_date").alias("shipped_year"), "order_id")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.max("cost").alias("max_cost"),
        F.count("*").alias("shipment_count"),
    )
    .filter(F.col("shipment_count") > 1)
    .drop("shipment_count")
    .orderBy("max_cost")
)

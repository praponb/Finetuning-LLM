from pyspark.sql import functions as F

with_dims = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.shipping_fee"), F.col("d.discount"))
)

result = (
    with_dims
    .withColumn("activity_month", F.date_trunc("MONTH", "delivered_date"))
    .groupingSets(
        [["activity_month", "status"], ["activity_month"], []],
        "activity_month",
        "status",
    )
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .orderBy(F.col("returned_count").desc_nulls_first())
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") != "Flash") | (F.col("status") != "RETURNED"))
    .groupBy(
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"),
        "order_id",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"),
    )
    .orderBy(F.col("order_id").desc(), F.col("dhl_count").desc())
)

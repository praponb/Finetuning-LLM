from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-08-09").cast("date")) & (F.col("carrier") != "Kerry"))
    .select(
        "*",
        F.date_format("delivered_date", "yyyy-MM-dd").alias("delivered_date_label"),
    )
)

result = (
    cleaned
    .groupBy("order_id")
    .pivot("status", ["IN_TRANSIT", "LOST", "RETURNED"])
    .agg(F.sum("cost").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "order_id",
        F.col("IN_TRANSIT_total").alias("in_transit_total"),
        F.col("IN_TRANSIT_cnt").alias("in_transit_cnt"),
        F.col("LOST_total").alias("lost_total"),
        F.col("LOST_cnt").alias("lost_cnt"),
        F.col("RETURNED_total").alias("returned_total"),
        F.col("RETURNED_cnt").alias("returned_cnt"),
    )
)

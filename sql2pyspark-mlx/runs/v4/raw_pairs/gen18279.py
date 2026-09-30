from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["IN_TRANSIT", "LOST", "RETURNED"])
    .agg(F.sum("cost").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "carrier",
        F.col("IN_TRANSIT_value_sum").alias("in_transit_value_sum"),
        F.col("IN_TRANSIT_item_count").alias("in_transit_item_count"),
        F.col("LOST_value_sum").alias("lost_value_sum"),
        F.col("LOST_item_count").alias("lost_item_count"),
        F.col("RETURNED_value_sum").alias("returned_value_sum"),
        F.col("RETURNED_item_count").alias("returned_item_count"),
    )
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "RETURNED") & F.col("order_id").isNotNull())
    .groupBy(
        "carrier",
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy(F.col("in_transit_cost").desc())
)

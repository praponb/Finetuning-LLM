from pyspark.sql import functions as F

base = df_shipments.filter((F.col("carrier") == "DHL") | (F.col("carrier") != "UPS"))

order_id_summary = (
    base
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
    )
)

result = (
    order_id_summary
    .select("order_id", "in_transit_cost")
    .orderBy(F.col("in_transit_cost").desc())
)

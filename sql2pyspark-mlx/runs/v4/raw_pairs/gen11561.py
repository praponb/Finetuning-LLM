from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST", "DELIVERED"]) & F.col("status").isin(["LOST", "DELIVERED"]) & (F.col("cost") < 56))
    .withColumn("shipped_date_month_end", F.last_day("shipped_date"))
)

carrier_summary = (
    src
    .groupBy("carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = carrier_summary.orderBy("carrier")

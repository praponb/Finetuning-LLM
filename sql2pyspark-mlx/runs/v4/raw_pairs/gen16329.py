from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([6, 11, 12]))
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
    .filter(F.col("cost_range") >= 58)
    .orderBy(F.col("status_group").desc())
    .limit(1)
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "status",
        "carrier",
        F.quarter("delivered_date").alias("delivered_quarter"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("pct_returned") < 62)
    .orderBy(F.col("stddev_cost").desc())
)

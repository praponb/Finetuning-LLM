from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

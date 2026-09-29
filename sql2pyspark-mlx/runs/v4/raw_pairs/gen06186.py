from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .groupBy("carrier", "status")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .orderBy("status")
)

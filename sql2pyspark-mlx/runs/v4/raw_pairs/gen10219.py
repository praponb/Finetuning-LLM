from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.year("shipped_date").alias("shipped_year"),
        F.quarter("delivered_date").alias("delivered_quarter"),
    )
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .orderBy("delivered_quarter")
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("delivered_date") >= 2023) & (F.col("cost") < 80))
    .groupBy(
        F.year("shipped_date").alias("shipped_year"),
        F.quarter("shipped_date").alias("shipped_quarter"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.max("cost").alias("max_cost"),
    )
)

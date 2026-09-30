from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([2, 4, 6]))
    .groupBy("carrier")
    .agg(
        F.count("*").alias("total_count"),
        F.min("shipped_date").alias("first_shipped_date"),
        F.count("*").alias("row_count"),
    )
)

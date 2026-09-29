from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("shipped_date").alias("event_date"))
    .unionByName(
        df_store_sales
        .filter(F.col("revenue") <= 4000)
        .select(F.col("sale_date").alias("event_date"))
    )
)

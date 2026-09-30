from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 45) & (F.col("cost") >= 91))
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(F.count("cost").alias("cost_count"))
)

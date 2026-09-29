from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([9, 10, 11]) & F.col("cost").between(47, 84))
    .agg(F.min("cost").alias("min_cost"))
)

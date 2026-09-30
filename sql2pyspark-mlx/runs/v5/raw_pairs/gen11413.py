from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2024-10-01")
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") > 49)
    .select("shipped_month", "max_cost")
)

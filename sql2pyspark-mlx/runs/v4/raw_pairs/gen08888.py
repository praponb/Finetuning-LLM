from pyspark.sql import functions as F

min_shipped_date = df_shipments.filter(F.col("delivered_date") < "2024-05-01").agg(F.min("shipped_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("shipped_date") == min_shipped_date)
    .select("id", "shipped_date", "status")
    .orderBy("status", "shipped_date")
)

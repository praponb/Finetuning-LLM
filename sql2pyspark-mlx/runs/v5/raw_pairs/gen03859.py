from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-08-01", "2023-09-07"))
    .select("id", "shipped_date", "delivered_date", "carrier")
)

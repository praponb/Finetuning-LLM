from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") < 52) & (F.col("shipped_date") > "2023-03-01"))
    .select("id", "status")
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2025)
    .select("carrier")
    .distinct()
)

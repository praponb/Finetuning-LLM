from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([2, 3, 12]) & (F.year("shipped_date") == 2025))
    .select("status", F.initcap(F.trim("carrier")).alias("carrier_title"))
)

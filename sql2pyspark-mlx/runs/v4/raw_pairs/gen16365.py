from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Berlin", "Chiang Mai", "New York"]))
    .select("opened_date")
    .union(df_shipments.select("delivered_date"))
)

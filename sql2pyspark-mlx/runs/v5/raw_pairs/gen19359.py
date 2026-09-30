from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["DE", "TH", "SG", "JP"])
    .agg(F.avg("rating"))
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["DE", "JP", "US", "TH"])
    .agg(F.avg("rating"))
    .orderBy("city")
)

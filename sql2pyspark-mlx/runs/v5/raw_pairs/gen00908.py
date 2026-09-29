from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["UK", "TH"])
    .agg(F.avg("rating"))
    .orderBy("city")
)

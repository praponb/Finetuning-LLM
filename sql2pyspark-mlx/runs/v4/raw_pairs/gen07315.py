from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "UK", "TH", "JP"])
    .agg(F.sum("rating"))
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["JP", "SG", "UK"])
    .agg(F.count("rating"))
)

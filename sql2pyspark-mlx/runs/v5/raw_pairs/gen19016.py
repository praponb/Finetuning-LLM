from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["TH", "SG"])
    .agg(F.avg("rating"))
)

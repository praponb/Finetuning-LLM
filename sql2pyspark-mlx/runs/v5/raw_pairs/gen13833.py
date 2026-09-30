from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "SG")
    .groupBy("city")
    .pivot("country", ["SG", "DE", "TH"])
    .agg(F.count("rating"))
    .orderBy("city")
)

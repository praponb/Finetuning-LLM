from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "Berlin"])
    .agg(F.max("rating"))
)

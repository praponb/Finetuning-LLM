from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Singapore", "Chiang Mai", "Berlin"])
    .agg(F.max("rating"))
    .orderBy("country")
)

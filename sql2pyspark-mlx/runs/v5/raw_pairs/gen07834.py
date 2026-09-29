from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "Bangkok"])
    .agg(F.sum("rating"))
    .orderBy("country")
)

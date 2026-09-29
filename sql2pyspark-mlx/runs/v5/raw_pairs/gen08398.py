from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Singapore", "London"])
    .agg(F.max("rating"))
)

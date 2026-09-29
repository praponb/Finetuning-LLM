from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Singapore", "New York", "London"])
    .agg(F.avg("rating"))
    .orderBy("country")
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Bangkok", "New York"])
    .agg(F.max("rating"))
)

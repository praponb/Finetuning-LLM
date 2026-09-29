from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Tokyo", "New York"])
    .agg(F.sum("rating"))
)

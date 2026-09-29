from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Berlin")
    .orderBy(F.col("id").desc())
)

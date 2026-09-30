from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Bangkok")
    .orderBy(F.col("supplier_name").desc(), "id")
    .limit(10)
)

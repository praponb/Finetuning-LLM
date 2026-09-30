from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Berlin"]))
    .orderBy(F.col("city").desc(), "supplier_name", "id")
    .limit(10)
)

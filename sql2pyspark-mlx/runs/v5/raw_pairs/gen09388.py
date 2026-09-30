from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .intersect(df_stores.select("city"))
    .orderBy(F.col("city").desc())
)

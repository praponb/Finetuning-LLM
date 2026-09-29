from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "rating")
    .orderBy(F.col("id").desc())
    .limit(3)
)

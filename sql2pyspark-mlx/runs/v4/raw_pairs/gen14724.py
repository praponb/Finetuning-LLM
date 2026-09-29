from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .distinct()
    .orderBy(F.col("status").desc())
)

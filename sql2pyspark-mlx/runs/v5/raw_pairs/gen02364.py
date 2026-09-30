from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 46)
    .select("status", "carrier")
    .distinct()
    .orderBy(F.col("carrier").desc(), "status")
    .limit(20)
)

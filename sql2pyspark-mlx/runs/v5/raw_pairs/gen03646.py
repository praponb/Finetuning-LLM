from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "RETURNED", "DELIVERED"]))
    .select("carrier")
    .distinct()
    .orderBy(F.col("carrier").desc())
)

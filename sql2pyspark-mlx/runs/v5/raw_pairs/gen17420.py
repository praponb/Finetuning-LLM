from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 83)
    .select("carrier")
    .distinct()
)

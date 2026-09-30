from pyspark.sql import functions as F

result = (
    df_shipments
    .select("carrier", "status")
    .distinct()
    .orderBy("carrier", "status")
    .limit(20)
)

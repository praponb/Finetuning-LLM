from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"))
    .filter(F.col("returned_count") > 3)
    .select("carrier", "returned_count")
    .orderBy(F.col("returned_count").desc())
)

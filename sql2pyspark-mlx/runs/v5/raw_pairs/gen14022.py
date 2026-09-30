from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Kerry"]))
    .agg(F.max("cost").alias("max_cost"))
    .orderBy(F.col("max_cost").desc())
)

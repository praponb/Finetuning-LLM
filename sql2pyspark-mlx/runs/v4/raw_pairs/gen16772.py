from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNotNull())
    .groupBy("carrier")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") > 28)
    .select("carrier", "max_cost")
    .orderBy(F.col("max_cost").desc(), "carrier")
    .limit(20)
)

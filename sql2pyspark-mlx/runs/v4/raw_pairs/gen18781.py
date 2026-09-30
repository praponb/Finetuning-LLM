from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.countDistinct("cost").alias("unique_statuss"))
    .filter(F.col("unique_statuss") >= 6)
    .orderBy("carrier")
)

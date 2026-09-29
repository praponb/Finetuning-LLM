from pyspark.sql import functions as F

min_cost = df_shipments.agg(F.min("cost")).first()[0]

result = (
    df_shipments
    .filter(F.col("cost") <= min_cost)
    .select("id", "cost", "carrier")
    .orderBy(F.col("cost").desc())
)

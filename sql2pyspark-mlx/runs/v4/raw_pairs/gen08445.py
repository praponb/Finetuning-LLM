from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "shipped_date", "status", F.ceil("cost").alias("cost_whole"))
)

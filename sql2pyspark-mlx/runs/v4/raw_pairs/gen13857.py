from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "shipped_date", F.substring("status", 1, 4).alias("status_prefix"))
    .orderBy(F.col("shipped_date").desc(), "status_prefix")
)

from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("cost") >= 72)

agg = (
    valid_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(F.min("shipped_date").alias("first_shipped_date"))
)

result = agg.select("shipped_year", "first_shipped_date")

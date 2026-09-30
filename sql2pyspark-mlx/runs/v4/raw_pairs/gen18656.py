from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") != "DHL") & F.col("cost").between(28, 55))
    .select("id", "delivered_date", F.upper("status").alias("status_upper"))
)

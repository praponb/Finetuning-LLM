from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .select("id", "cost")
)

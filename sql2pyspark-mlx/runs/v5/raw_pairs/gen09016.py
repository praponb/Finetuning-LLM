from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", "country", F.col("rating").cast("int").alias("rating_int"))
)

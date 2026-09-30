from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "channel")
    .orderBy(F.col("id").desc())
)

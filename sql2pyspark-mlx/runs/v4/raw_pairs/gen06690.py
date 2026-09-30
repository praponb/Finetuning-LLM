from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .pivot("segment", ["Corporate", "SMB"])
    .agg(F.avg("age"))
    .orderBy("is_active")
)

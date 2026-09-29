from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("segment", ["Corporate", "Consumer", "SMB"])
    .agg(F.max("age"))
    .orderBy("country")
)

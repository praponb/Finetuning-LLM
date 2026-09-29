from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("segment", ["SMB", "Corporate"])
    .agg(F.max("credit_limit"))
    .orderBy("country")
)

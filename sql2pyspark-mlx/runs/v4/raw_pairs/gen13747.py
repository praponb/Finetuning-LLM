from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("segment", ["Consumer", "SMB"])
    .agg(F.count("credit_limit"))
)

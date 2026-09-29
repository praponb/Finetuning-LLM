from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .groupBy("country")
    .pivot("segment", ["SMB", "Consumer", "Corporate"])
    .agg(F.avg("age"))
)

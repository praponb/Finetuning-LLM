from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["TH", "SG", "US", "DE"])
    .agg(F.max("credit_limit"))
)

from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("country").isin(["DE", "TH"]), "core").otherwise("other").alias("country_group"))
    .pivot("city", ["Tokyo", "Berlin", "New York", "Singapore"])
    .agg(F.max("credit_limit"))
)

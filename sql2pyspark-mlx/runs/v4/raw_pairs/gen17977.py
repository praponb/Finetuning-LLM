from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") > "2023-07-01") & F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
)

from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("age") != 44) & (~F.col("segment").isin(["Corporate", "Consumer"]) | F.col("segment").isin(["Consumer", "SMB"])))
)

agg = (
    recent_customers
    .groupBy("country")
    .agg(F.min("signup_date").alias("first_signup_date"))
)

result = agg

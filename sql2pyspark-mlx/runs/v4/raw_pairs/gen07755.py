from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.col("segment") == "Consumer") & F.col("segment").isNull()))
    .select("is_active")
)

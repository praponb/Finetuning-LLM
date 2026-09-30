from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "Chiang Mai") & ((F.col("age") >= 40) | ~F.col("is_active")))
    .orderBy("country")
)

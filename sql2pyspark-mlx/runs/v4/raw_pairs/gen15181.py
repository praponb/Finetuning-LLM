from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select(
        "id",
        "credit_limit",
        "city",
        F.when(F.col("age") >= 71, "High").when(F.col("age") >= 30, "Medium").otherwise("Low").alias("age_band"),
    )
    .orderBy("city")
)

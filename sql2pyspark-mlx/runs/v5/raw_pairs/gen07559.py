from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(8700, 11900))
    .select(
        "city",
        "name",
        "age",
        F.concat("segment", F.lit(", "), "email").alias("segment_email"),
        F.when(F.col("age") >= 67, "High").when(F.col("age") >= 30, "Medium").otherwise("Low").alias("age_level"),
    )
)

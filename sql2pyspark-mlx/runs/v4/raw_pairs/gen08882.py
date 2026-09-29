from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "sqft",
        "opened_date",
        F.when(F.col("sqft") >= 13231, "High").when(F.col("sqft") >= 7870, "Medium").otherwise("Low").alias("sqft_level"),
    )
    .orderBy(F.col("sqft_level").desc(), "sqft")
)

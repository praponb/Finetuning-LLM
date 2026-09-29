from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("country") == "JP") | (F.col("country") == "SG")))
    .select(
        "country",
        F.when(F.col("rating") >= 4.0, "High").when(F.col("rating") >= 2.2, "Medium").otherwise("Low").alias("rating_tier"),
    )
)

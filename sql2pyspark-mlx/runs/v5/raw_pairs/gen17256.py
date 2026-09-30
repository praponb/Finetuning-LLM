from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "country",
        F.when(F.col("rating") >= 3.7, "High").when(F.col("rating") >= 2.1, "Medium").otherwise("Low").alias("rating_level"),
    )
)

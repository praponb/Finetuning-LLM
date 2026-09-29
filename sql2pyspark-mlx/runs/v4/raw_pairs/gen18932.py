from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
    )
)

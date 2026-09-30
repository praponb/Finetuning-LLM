from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "country",
        "supplier_name",
        F.col("rating").cast("int").alias("rating_int"),
    )
)

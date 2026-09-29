from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        F.col("rating").cast("int").alias("rating_int"),
        F.ceil("rating").alias("rating_whole"),
    )
)

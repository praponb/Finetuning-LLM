from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Tokyo%"))
    .select(
        "id",
        "city",
        "supplier_name",
        "rating",
        F.upper("city").alias("city_upper"),
        F.ceil("rating").alias("rating_whole"),
    )
)

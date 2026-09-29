from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Sia%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
    )
)

result = (
    cleaned
    .select("city", "id")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "US")
        .select("city", "id")
    )
)

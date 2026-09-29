from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)

result = (
    valid
    .select("country", "id")
    .intersectAll(
        df_suppliers
        .filter(F.col("country") == "TH")
        .select("country", "id")
    )
)

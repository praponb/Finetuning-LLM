from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(~F.col("country").isin(["TH", "DE", "SG"]))
    .select(
        "*",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

result = (
    valid
    .select("city", "id")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "London")
        .select("city", "id")
    )
)

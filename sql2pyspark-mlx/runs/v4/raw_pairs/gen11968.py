from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.1) & (F.col("rating") > 3.1))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.initcap(F.trim("city")).alias("city_title"),
    )
    .orderBy("rating", "supplier_name")
)

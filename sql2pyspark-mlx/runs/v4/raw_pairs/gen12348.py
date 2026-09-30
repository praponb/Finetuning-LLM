from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select("city", "country")
    .unionByName(
        df_suppliers
        .filter(F.lower(F.trim("supplier_name")) == "pacific wholesale")
        .select("city", "country")
    )
    .distinct()
    .orderBy("country")
)

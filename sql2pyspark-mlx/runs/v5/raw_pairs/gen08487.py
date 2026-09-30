from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("Tok%"))
    .select("id", "supplier_name", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("city") == "New York")
        .select("id", "supplier_name", "city")
    )
    .distinct()
    .orderBy(F.col("supplier_name").desc())
)

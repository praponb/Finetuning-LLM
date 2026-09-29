from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%URO%"))
    .select("country")
    .unionByName(
        df_suppliers
        .filter(~F.col("supplier_name").like("Tok%"))
        .select("country")
    )
    .distinct()
)

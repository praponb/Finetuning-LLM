from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eur%"))
    .select("country", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("country") != "US")
        .select("country", "supplier_name")
    )
    .orderBy("supplier_name")
)

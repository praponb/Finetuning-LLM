from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 4.1)
    .select("country", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("city") == "Berlin")
        .select("country", "supplier_name")
    )
    .orderBy("supplier_name")
)

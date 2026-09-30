from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.5)
    .select("id", "supplier_name", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("rating") < 2.7)
        .select("id", "supplier_name", "country")
    )
    .distinct()
    .orderBy("supplier_name")
)

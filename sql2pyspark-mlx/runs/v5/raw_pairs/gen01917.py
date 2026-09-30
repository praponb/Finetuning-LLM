from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.0, 4.2))
    .select("supplier_name", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("rating") >= 2.0)
        .select("supplier_name", "country")
    )
    .distinct()
    .orderBy("country")
)

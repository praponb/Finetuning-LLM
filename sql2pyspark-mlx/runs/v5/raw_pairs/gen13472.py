from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNull())
    .select("city", "supplier_name")
    .unionByName(
        df_suppliers
        .filter(F.col("rating") <= 2.4)
        .select("city", "supplier_name")
    )
    .distinct()
    .orderBy("city")
)

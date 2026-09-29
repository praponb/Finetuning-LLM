from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Glo%"))
    .select("id", "supplier_name", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isNotNull())
        .select("id", "supplier_name", "country")
    )
    .distinct()
    .orderBy("id")
)

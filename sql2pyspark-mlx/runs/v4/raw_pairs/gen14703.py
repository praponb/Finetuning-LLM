from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("id", "city", "country")
    .unionByName(
        df_suppliers
        .filter(~F.col("supplier_name").like("P%"))
        .select("id", "city", "country")
    )
    .distinct()
    .orderBy("country")
)

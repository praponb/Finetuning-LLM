from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.7)
    .select("city", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").isNotNull())
        .select("city", "country")
    )
    .orderBy("city")
)

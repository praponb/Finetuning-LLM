from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNotNull())
    .select("country")
    .intersect(
        df_suppliers
        .filter(F.col("supplier_name").like("%Sup%"))
        .select("country")
    )
    .orderBy("country")
)

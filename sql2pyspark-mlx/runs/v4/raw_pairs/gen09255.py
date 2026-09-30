from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("country", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").isNull())
        .select("country", "city")
    )
    .distinct()
    .orderBy("country")
)

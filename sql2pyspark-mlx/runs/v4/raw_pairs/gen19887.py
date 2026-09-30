from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") > 10)
    .select("city", "country")
    .intersect(
        df_suppliers
        .filter(F.length("supplier_name") >= 11)
        .select("city", "country")
    )
    .orderBy(F.col("country").desc())
)

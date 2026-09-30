from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") >= 11)
    .select("country")
    .unionByName(
        df_suppliers
        .filter(F.col("rating").between(2.3, 3.4))
        .select("country")
    )
    .orderBy(F.col("country").desc())
)

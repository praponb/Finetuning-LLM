from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") >= 10)
    .select("id", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("rating").between(3.6, 4.1))
        .select("id", "city")
    )
    .distinct()
    .orderBy("id")
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Berlin", "New York", "Bangkok"]))
    .select("supplier_name", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("country").isin(["JP", "DE"]))
        .select("supplier_name", "city")
    )
    .distinct()
)

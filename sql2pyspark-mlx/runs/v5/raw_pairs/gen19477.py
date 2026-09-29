from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select("country")
    .unionByName(
        df_suppliers
        .filter(F.upper(F.trim("supplier_name")) == "TOKYO TRADERS")
        .select("country")
    )
    .distinct()
    .orderBy(F.col("country").desc())
)

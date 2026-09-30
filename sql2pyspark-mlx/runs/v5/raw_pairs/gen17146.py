from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .select("id", "country")
    .unionByName(
        df_suppliers
        .filter(~F.col("country").isin(["SG", "DE"]))
        .select("id", "country")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)

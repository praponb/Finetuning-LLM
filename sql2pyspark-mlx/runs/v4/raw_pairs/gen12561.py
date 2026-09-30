from pyspark.sql import functions as F

result = (
    df_departments
    .select("location")
    .union(
        df_suppliers
        .filter(~F.col("country").isin(["UK", "DE", "SG"]))
        .select("city")
    )
    .distinct()
    .orderBy("location")
)

from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city")
    .unionByName(
        df_suppliers
        .filter(F.col("country").isin(["DE", "SG", "JP"]))
        .select("country", "city")
    )
    .orderBy("city")
)

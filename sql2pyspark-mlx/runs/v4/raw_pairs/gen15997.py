from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Singapore", "Tokyo"]))
    .select("id", "city", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("rating") > 3.2)
        .select("id", "city", "country")
    )
)

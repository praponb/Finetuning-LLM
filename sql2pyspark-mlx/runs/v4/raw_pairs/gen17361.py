from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.4)
    .select("city", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("country") == "US")
        .select("city", "country")
    )
    .orderBy("city")
)

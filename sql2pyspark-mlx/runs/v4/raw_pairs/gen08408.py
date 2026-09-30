from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Chiang Mai", "London"]))
    .select("id", "country")
    .unionByName(df_suppliers.filter(F.col("rating") >= 2.6).select("id", "country"))
    .orderBy(F.col("id").desc())
)

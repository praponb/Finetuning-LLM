from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "US"]) & (F.col("rating") >= 4.1))
    .select("id", "country")
    .orderBy("country", F.col("id").desc())
    .limit(20)
)

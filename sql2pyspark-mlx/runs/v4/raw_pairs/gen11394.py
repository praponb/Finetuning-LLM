from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "TH") & F.col("country").isin(["TH", "SG", "US"]))
    .select("id", "city", "country", "rating")
    .orderBy("rating")
)

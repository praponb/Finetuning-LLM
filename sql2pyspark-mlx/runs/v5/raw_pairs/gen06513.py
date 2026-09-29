from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 2.1) & F.col("country").isin(["UK", "SG", "TH"]))
    .select("id", "city", "country")
)

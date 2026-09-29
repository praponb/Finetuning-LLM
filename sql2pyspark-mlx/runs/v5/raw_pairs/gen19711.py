from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "SG", "JP"]) & ((F.col("country") == "SG") | F.col("country").isin(["JP", "DE"])))
    .select("country")
)

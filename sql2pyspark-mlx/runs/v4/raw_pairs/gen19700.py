from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "DE"]) | F.col("country").isin(["US", "TH"]))
    .select("rating", "supplier_name", "country")
    .orderBy("country", "supplier_name")
)

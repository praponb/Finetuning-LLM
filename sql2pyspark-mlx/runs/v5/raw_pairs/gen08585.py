from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.9) & F.col("country").isin(["UK", "TH"]))
    .select("id", "supplier_name", "city")
)

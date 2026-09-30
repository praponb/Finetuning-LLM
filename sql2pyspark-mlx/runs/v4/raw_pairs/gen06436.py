from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Berlin", "New York"]) & F.col("country").isin(["UK", "TH"]))
    .select("country", "supplier_name")
)

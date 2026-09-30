from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "US", "SG"]) & (F.length("supplier_name") < 14))
)

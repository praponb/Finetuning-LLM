from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "UK", "TH"]))
    .select("supplier_name", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["New York", "Singapore"]))
        .select("supplier_name", "country")
    )
)

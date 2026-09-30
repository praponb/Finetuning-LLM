from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Bangkok", "Singapore"]))
    .select("supplier_name", "country")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["US", "TH", "JP"]))
        .select("supplier_name", "country")
    )
)

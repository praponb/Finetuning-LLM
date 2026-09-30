from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "DE", "US"]))
    .select("id", "supplier_name", "country")
    .subtract(
        df_suppliers
        .filter(F.col("city").isin(["London", "New York", "Chiang Mai"]))
        .select("id", "supplier_name", "country")
    )
)

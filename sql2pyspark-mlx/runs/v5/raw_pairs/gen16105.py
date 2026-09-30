from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select("id", "city")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("Lio%"))
        .select("id", "city")
    )
)

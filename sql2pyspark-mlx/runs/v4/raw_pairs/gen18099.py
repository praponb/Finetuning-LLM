from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", "country")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("Pac%"))
        .select("city", "country")
    )
)

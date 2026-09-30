from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("city") == "Berlin") & (F.col("supplier_name").like("Si%") | F.col("country").isin(["TH", "DE", "UK"]))))
    .select(
        "city",
        "country",
        "supplier_name",
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
    .orderBy("country")
)

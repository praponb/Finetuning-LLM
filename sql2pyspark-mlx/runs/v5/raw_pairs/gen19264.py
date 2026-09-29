from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Singapore"]) & ((F.col("city") == "Bangkok") | F.col("supplier_name").isNull()))
    .select(
        "id",
        "supplier_name",
        "city",
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        F.when(F.col("brand") == "Acme", 1).otherwise(0).alias("is_acme"),
    )
    .orderBy(F.col("id").desc())
)

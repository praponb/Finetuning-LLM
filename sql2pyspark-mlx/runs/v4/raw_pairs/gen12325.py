from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("brand").isin(["Umbrella", "Acme", "Initech"]))
        .select("id", "supplier_id")
    )
)

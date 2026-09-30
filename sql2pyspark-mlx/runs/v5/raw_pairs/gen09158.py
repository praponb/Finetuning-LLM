from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 370)
    .select("id", "supplier_id", "category")
    .unionByName(
        df_products
        .filter(F.col("launch_date") < "2023-09-19")
        .select("id", "supplier_id", "category")
    )
    .orderBy(F.col("id").desc())
)

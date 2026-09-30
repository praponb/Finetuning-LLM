from pyspark.sql import functions as F

result = (
    df_products
    .select("supplier_id", "category")
    .exceptAll(
        df_products
        .filter(F.col("stock_qty") >= 110)
        .select("supplier_id", "category")
    )
)

from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Fashion")
    .select("id", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("stock_qty") != 223)
        .select("id", "supplier_id")
    )
)

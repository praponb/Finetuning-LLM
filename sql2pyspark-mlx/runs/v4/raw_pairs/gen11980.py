from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Umbrella", "Acme"]))
    .select("stock_qty")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isin([22, 38]))
        .select("stock_qty")
    )
    .orderBy("stock_qty")
)

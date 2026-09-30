from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([11, 24]))
    .select("supplier_id")
    .unionByName(
        df_products
        .filter(F.col("category") == "Sports")
        .select("supplier_id")
    )
    .orderBy("supplier_id")
)

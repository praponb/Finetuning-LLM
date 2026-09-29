from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") != 190)
        .select(F.col("last_restock").alias("sale_date"))
    )
    .distinct()
    .orderBy("sale_date")
)

from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") >= F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("brand").rlike("^(Initech|Globex)$"), "OTHER_ZONE").when(F.col("brand").rlike("^(Acme|Umbrella)$"), "EXTENDED").otherwise("GROUP_A").alias("brand_zone"),
    )
    .orderBy(F.col("brand").asc_nulls_last())
)

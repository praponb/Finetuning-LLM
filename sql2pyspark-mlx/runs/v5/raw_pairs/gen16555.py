from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("price").between(690, 760))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.date_format("launch_date", "yyyy-MM-dd").alias("launch_date_label"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.launch_date")), "left_anti")
)

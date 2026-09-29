from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category")
w_hist = Window.orderBy("price", "id")
w3 = Window.partitionBy("brand")
w_hist_2 = Window.orderBy(F.col("stock_qty").desc(), "id")
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

with_windows = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "supplier_id",
        F.min("price").over(w).alias("brand_min_price"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
        F.row_number().over(w_hist_2).alias("row_seq"),
    )
)

result = (
    with_windows
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "supplier_id",
        "brand_min_price",
        "seq_no",
        "price_share",
        F.when(F.col("category").rlike("^(Electronics|Home)$"), "PRIORITY").when(F.col("category").rlike("^(Sports|Books)$"), "EXTENDED").otherwise("GROUP_A").alias("category_zone"),
        F.col("max_product_price"),
    )
    .orderBy(F.col("supplier_id").desc())
)

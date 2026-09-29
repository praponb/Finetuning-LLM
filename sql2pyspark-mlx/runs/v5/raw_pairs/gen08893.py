from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("units").alias("max_store_sale_units"))

scored = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.max("quantity").over(w).alias("store_id_max_quantity"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    scored
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id_max_quantity",
        "event_seq",
        F.col("max_store_sale_units"),
    )
)

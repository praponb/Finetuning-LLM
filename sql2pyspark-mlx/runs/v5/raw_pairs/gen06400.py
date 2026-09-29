from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "RUNNING SHOES")
    .select(
        "id",
        "supplier_id",
        "price",
        F.max("price").over(w).alias("supplier_id_max_price"),
    )
)

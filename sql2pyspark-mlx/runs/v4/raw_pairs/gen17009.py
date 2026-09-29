from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_inventory
    .filter(F.col("quantity").between(118, 178))
    .select(
        "id",
        "product_id",
        "quantity",
        F.max("quantity").over(w).alias("product_id_max_quantity"),
        F.count("*").over(w).alias("product_id_count"),
    )
    .orderBy(F.col("product_id").desc(), F.col("product_id_count").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_inventory
    .filter(F.col("product_id") == 8)
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.last("quantity").over(w).alias("last_quantity"),
    )
    .orderBy(F.col("id").desc())
)

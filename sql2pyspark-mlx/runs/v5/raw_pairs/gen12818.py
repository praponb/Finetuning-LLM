from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("brand") != "Globex")
    .select(
        "id",
        "supplier_id",
        "price",
        "launch_date",
        F.first("launch_date").over(w).alias("first_launch_date"),
    )
    .orderBy("price", F.col("id").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "price",
        F.lead("price", 1).over(w).alias("next_price"),
        F.percent_rank().over(w2).alias("launch_date_pct_rank"),
    )
)

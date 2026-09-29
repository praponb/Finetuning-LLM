from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date")

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "cost",
        "supplier_id",
        F.lead("cost", 2).over(w).alias("next_cost"),
        F.percent_rank().over(w2).alias("launch_date_pct_rank"),
    )
    .orderBy(F.col("supplier_id").desc())
)

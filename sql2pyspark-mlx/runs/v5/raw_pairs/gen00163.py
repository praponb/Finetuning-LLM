from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("supplier_id") == 27)
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
)

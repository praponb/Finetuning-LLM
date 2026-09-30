from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
    )
)

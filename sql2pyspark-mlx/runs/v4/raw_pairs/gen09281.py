from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("category").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.month("launch_date").isin([3, 8, 9]))
    .select(
        "id",
        "brand",
        "launch_date",
        "cost",
        "category",
        F.lead("cost", 2).over(w).alias("next_cost"),
        F.percent_rank().over(w2).alias("cost_pct_rank"),
    )
)

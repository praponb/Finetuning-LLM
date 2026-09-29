from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date")

result = (
    df_products
    .filter(F.col("price") > 870)
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        "brand",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("rnk").desc(), F.col("next_cost").desc())
)

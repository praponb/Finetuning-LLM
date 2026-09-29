from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("launch_date").between("2024-10-01", "2025-09-01"))
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
    )
)

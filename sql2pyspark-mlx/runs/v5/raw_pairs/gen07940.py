from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("supplier_id")

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .select(
        "id",
        "stock_qty",
        "launch_date",
        F.sum("cost").over(w).alias("last3_sum_cost"),
        F.count("cost").over(w).alias("last3_count_cost"),
        F.avg("cost").over(w).alias("last3_avg_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

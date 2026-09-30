from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").asc_nulls_first())
w2 = Window.orderBy("revenue", "id")
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        F.dense_rank().over(w).alias("revenue_dense_rank"),
        F.lag("revenue", 1).over(w2).alias("prev_revenue"),
        F.first("revenue", ignorenulls=True).over(w3).alias("first_known_revenue"),
    )
)

result = (
    scored
    .select("id", "product_id", "revenue_dense_rank", "prev_revenue")
    .orderBy("revenue_dense_rank")
)

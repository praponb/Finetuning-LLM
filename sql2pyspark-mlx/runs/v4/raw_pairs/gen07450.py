from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("category", "supplier_id").orderBy("cost", "id")
w2 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-3, -1)
w_part = Window.partitionBy("brand").orderBy("stock_qty", "id")

enriched = (
    df_products
    .select(
        "*",
        F.ntile(5).over(w_ordered).alias("cost_bucket"),
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_3_prior"),
        F.lead("price", 1).over(w_part).alias("next_price"),
    )
)

result = (
    enriched
    .groupBy("brand")
    .agg(
        F.max(F.when(F.col("brand") == "Globex", F.col("price"))).alias("globex_max_price"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.countDistinct("brand").alias("distinct_brands_2"),
    )
    .filter(F.col("globex_max_price") >= 0)
    .orderBy("globex_max_price")
)

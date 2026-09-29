from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, -1)
w_hist = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w_hist_2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())

scored = (
    df_inventory
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w))
    .withColumn("quantity_avg_6", F.avg("quantity").over(w_hist))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w_hist_2))
)

result = (
    scored
    .select(
        "id",
        "quantity",
        "quantity_avg_6_prior",
        "quantity_avg_6",
        "quantity_cume_dist",
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)

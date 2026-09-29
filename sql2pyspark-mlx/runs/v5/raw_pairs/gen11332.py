from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id")

scored = (
    df_products
    .filter(F.year("launch_date") >= 2023)
    .withColumn("next_cost", F.lead("cost").over(w))
    .withColumn("prev_cost", F.lag("cost").over(w))
    .withColumn("prev_price", F.lag("price").over(w))
    .withColumn("next_price", F.lead("price").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

result = (
    scored
    .groupBy("category")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("category").desc_nulls_first())
)

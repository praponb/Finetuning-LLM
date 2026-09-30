from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy(F.col("launch_date").desc())

ranked = (
    df_products
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .withColumn("cost_tier", F.when(F.col("cost") >= 670, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("quarter", "launch_date"))
    .groupingSets(
        [["cost_tier", "quarter_start"], ["cost_tier"]],
        "cost_tier",
        "quarter_start",
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("price")).otherwise(0)).alias("umbrella_price"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.grouping("cost_tier").alias("grouping_level"),
    )
)

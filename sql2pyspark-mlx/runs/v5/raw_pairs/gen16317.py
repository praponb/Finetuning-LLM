from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("cost", "id")

ranked = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .withColumn("cost_tier", F.when(F.col("cost") >= 680, "HIGH").otherwise("LOW"))
    .rollup("cost_tier", "supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("cost")).otherwise(0)).alias("home_cost"),
        F.stddev_samp("price").alias("price_stddev"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("home_cost").desc())
)

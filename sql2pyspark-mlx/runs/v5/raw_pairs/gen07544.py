from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        F.col("c.quantity"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("quantity") >= 252, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.avg(F.when(F.col("category") == "Grocery", F.col("quantity"))).alias("grocery_avg_quantity"),
        F.count("*").alias("cnt"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.category"),
    )
)

result = (
    latest_match
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.count("*").alias("cnt"),
    )
)

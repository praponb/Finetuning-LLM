from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter((F.col("stock_qty").between(382, 396) & F.col("launch_date").between("2023-04-01", "2023-09-22")) | (F.col("category") == "Grocery"))
    .withColumn("price_whole", F.floor("price"))
)

summary = (
    valid_products
    .groupBy("supplier_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    summary
    .select("supplier_id", "max_cost")
    .orderBy("max_cost", F.col("supplier_id").desc())
)

from pyspark.sql import functions as F

src = (
    df_products
    .filter(((F.col("category") == "Grocery") & F.col("category").isin(["Home", "Fashion", "Books"])) | (F.col("stock_qty") != 355))
)

products_stats = (
    src
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = products_stats.orderBy(F.col("avg_cost").desc(), "launch_year")

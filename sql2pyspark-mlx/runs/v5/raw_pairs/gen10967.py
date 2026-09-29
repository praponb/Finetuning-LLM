from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        F.col("c.units"),
        F.col("c.revenue"),
    )
)

result = (
    with_latest
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_rows"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
    )
)

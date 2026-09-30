from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-04-20"))) & F.col("supplier_id").isNotNull())
    .withColumn("launch_date_month_end", F.last_day("launch_date"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

result = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

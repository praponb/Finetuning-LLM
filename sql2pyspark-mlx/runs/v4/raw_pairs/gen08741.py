from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2024-02-22").cast("date"))
    .select("*", F.add_months("launch_date", 1).alias("launch_date_plus_months"))
)

with_last = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

kept = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.launch_date_plus_months")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "supplier_id",
        "launch_date_plus_months",
        "order_item_quantity",
        F.when(F.col("category").rlike("^(Sports|Grocery)$"), "GROUP_A").when(F.col("category").rlike("^(Electronics|Home)$"), "CORE").otherwise("EXTENDED").alias("category_zone"),
    )
    .orderBy(F.col("category_zone").desc())
)

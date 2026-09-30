from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("margin", "id")
w2 = Window.partitionBy("brand")

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-07-12").cast("date")) & F.col("supplier_id").isNotNull() & (F.upper(F.trim("product_name")) == "LAPTOP PRO 14"))
    .select("*", (F.col("price") + F.col("cost")).alias("margin"))
)

scored = (
    filtered
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
)

kept = (
    scored
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity").between(109, 138)), "left_anti")
)

summary = (
    kept
    .groupBy("brand")
    .agg(
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("cost_per_grocery"),
        F.min("price").alias("min_price"),
    )
    .filter(F.col("cost_per_grocery") < 0)
)

result = (
    summary
    .select(
        "brand",
        "cost_per_grocery",
        "min_price",
        F.when(F.col("brand").rlike("^(Stark|Globex)$"), "PRIORITY").when(F.col("brand").rlike("^(Acme|Initech)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("brand_zone"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-02-24").cast("date")) & F.col("product_id").isin([6, 8]))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 246, "High").when(F.col("quantity") >= 113, "Medium").otherwise("Low"))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

result = (
    filtered
    .select(
        "id",
        "product_id",
        "quantity_level",
        "days_since_last_restock",
        F.count("days_since_last_restock").over(w).alias("running_count_days_since_last_restock"),
        F.avg("days_since_last_restock").over(w).alias("running_avg_days_since_last_restock"),
        F.max("days_since_last_restock").over(w).alias("running_max_days_since_last_restock"),
    )
)

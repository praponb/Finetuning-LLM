from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("quantity", "id")
w3 = Window.orderBy("last_restock_period", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("last_known_quantity", "id")
w5 = Window.orderBy("quantity_avg_2_prior", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-02-13").cast("date"))
    .select(
        "id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 198, "High").when(F.col("quantity") >= 97, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

cleaned_inventory = (
    cleaned
    .filter(F.col("last_restock") > F.lit("2023-03-21").cast("date"))
    .select("*", F.date_trunc("YEAR", "last_restock").alias("last_restock_period"))
)

with_windows = (
    cleaned_inventory
    .filter(F.hour("last_restock_period").between(7, 10))
    .select(
        "id",
        "quantity_band",
        F.last("quantity", ignorenulls=True).over(w_ordered).alias("last_known_quantity"),
        F.lag("quantity", 2).over(w_hist).alias("prev_quantity"),
        F.avg("quantity").over(w3).alias("quantity_avg_2_prior"),
    )
)

with_windows_inventory = (
    with_windows
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("quantity_avg_2_prior_avg_2", F.avg("quantity_avg_2_prior").over(w5))
)

result = (
    with_windows_inventory
    .groupBy(
        F.when(F.col("quantity_avg_2_prior_avg_2") >= 206, "PREMIUM").otherwise("STANDARD").alias("quantity_avg_2_prior_avg_2_tier"),
        F.when(F.col("prev_quantity") >= 257, "PREMIUM").otherwise("STANDARD").alias("prev_quantity_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("last_known_quantity"), 2).alias("avg_last_known_quantity"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("quantity_avg_2_prior_avg_2_tier").asc_nulls_last())
)

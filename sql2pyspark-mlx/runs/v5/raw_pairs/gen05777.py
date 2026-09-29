from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("supplier_id", "brand").orderBy("cost", "id")
w_ordered = Window.partitionBy("supplier_id", "brand").orderBy("stock_qty", "id").rowsBetween(-3, -1)

scored = (
    df_products
    .filter(F.col("cost").between(270, 300))
    .withColumn("next_cost", F.lead("cost").over(w_part))
    .withColumn("prev_cost", F.lag("cost").over(w_part))
    .withColumn("prev_price", F.lag("price").over(w_part))
    .withColumn("next_price", F.lead("price").over(w_part))
    .withColumn("row_seq", F.row_number().over(w_part))
    .withColumn("price_avg_3_prior", F.avg("price").over(w_ordered))
)

result = (
    scored
    .groupBy(
        "supplier_id",
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.count_if(F.col("brand") == "Acme").alias("acme_events"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.percentile_approx("next_price", [0.25, 0.5, 0.75]).alias("next_price_quartiles"),
        F.percentile_approx("prev_price", [0.25, 0.5, 0.75]).alias("prev_price_quartiles"),
    )
)

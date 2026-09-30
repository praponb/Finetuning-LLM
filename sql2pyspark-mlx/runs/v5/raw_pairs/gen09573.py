from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")

scored = (
    df_order_items
    .select(
        "*",
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("row_seq"),
    )
)

result = (
    scored
    .cube(
        "order_id",
        F.when(F.col("unit_price") >= 330, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count("*").alias("cnt"),
        F.round(F.avg("prev_quantity"), 2).alias("avg_prev_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("prev_quantity"), 2).alias("avg_prev_quantity_2"),
    )
    .orderBy(F.col("quantity_stddev").desc())
)

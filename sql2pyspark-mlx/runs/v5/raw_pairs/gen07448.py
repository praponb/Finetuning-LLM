from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 4)
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

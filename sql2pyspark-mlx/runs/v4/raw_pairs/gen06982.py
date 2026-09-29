from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "channel", "discount", "shipping_fee", "amount")
    .unpivot(["id", "channel"], ["discount", "shipping_fee", "amount"], "metric", "val")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "method").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "amount",
        "paid_at",
        F.lead("amount").over(w_grp).alias("next_amount"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

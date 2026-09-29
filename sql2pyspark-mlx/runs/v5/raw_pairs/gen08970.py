from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.lead("quantity").over(w_grp).alias("next_quantity"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

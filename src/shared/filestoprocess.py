from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

class FilesToProcess:
    @staticmethod
    def process_bronze_member_stream(
        raw_df: DataFrame, 
        client_id: str = "TESTCLIENT",
        file_layout_id: str = "834",
        file_layout_description: str = "Standard834"
    ) -> DataFrame:
        """
        Applies transformation logic to incoming member records vectorially.
        Eliminates all row-level loops, JSON string builders, and file movement checks.
        """
        # 1. Drop internal filler columns dynamically
        non_filler_cols = [c for c in raw_df.columns if not c.startswith("Filler_")]
        df_filtered = raw_df.select(non_filler_cols)

        file_id_expr = col("FILE_ID") if "FILE_ID" in df_filtered.columns else col("_metadata.file_name")

        # 2. Extract metadata and add system tracking columns
        return (
            df_filtered
            .withColumn("CLIENT_ID", lit(client_id))
            .withColumn("FILE_LAYOUT_ID", lit(file_layout_id))
            .withColumn("FILE_LAYOUT_DESCRIPTION", lit(file_layout_description))
            .withColumn("FILE_ID", file_id_expr)
            .withColumn("LOAD_DATETIME", current_timestamp())
        )
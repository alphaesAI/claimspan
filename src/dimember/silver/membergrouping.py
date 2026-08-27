import recordlinkage
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, col, row_number, substring, 
    upper, trim, regexp_replace, expr, to_json, struct, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

# Matching rules definitions
RuleMBIColumns = ["identifier_beneficiaryID", "name_given_first_initial", "name_family_initial", "birthDate_formatted", "telecom_phone_home_formatted", "address_permanent_line1", 11]
RulePMIDColumns = ["identifier_planMemberID", "name_given_first_initial", "name_family_initial", "birthDate_formatted", "telecom_phone_home_formatted", "address_permanent_line1", 11]
RuleUPKColumns = ["identifier_uniquepersonkey", "name_given_first_initial", "name_family_initial", "birthDate_formatted", "telecom_phone_home_formatted", "address_permanent_line1", 11]
RuleOtherColumns = ["birthDate_formatted", "name_given_first", "name_family", "telecom_phone_home_formatted", "address_permanent_line1", 14]

RulesAll = [RuleMBIColumns, RulePMIDColumns, RuleUPKColumns, RuleOtherColumns]
CompareColumns = [
    "name_family_initial", "name_given_first_initial", "name_family", "name_given_first", "identifier_beneficiaryID", 
    "identifier_planMemberID", "identifier_uniquepersonkey", "birthDate_formatted", "telecom_phone_home_formatted", "address_permanent_line1"
]

# SQL Transformation Logic
FINAL_SQL = """
WITH ESAIPersonWithIdentifiers AS (
  SELECT 
     UniqueRecord, FileLayoutID, FileID, RowNumber, name_family, name_given_first, birthDate, gender, 
     address_permanent_line1, telecom_phone_home, identifier_planMemberID, identifier_beneficiaryID, identifier_uniquepersonkey,
     CASE WHEN INSTR(MatchID, ',') = 0 THEN MatchID ELSE SUBSTR(MatchID, 1, INSTR(MatchID, ',') - 1) END AS MatchID,
     ROW_NUMBER() OVER(PARTITION BY (CASE WHEN INSTR(MatchID, ',') = 0 THEN MatchID ELSE SUBSTR(MatchID, 1, INSTR(MatchID, ',') - 1) END) ORDER BY FileID ASC, RowNumber ASC) AS FirstPersonIdentifier,
     ROW_NUMBER() OVER(PARTITION BY (CASE WHEN INSTR(MatchID, ',') = 0 THEN MatchID ELSE SUBSTR(MatchID, 1, INSTR(MatchID, ',') - 1) END) ORDER BY FileID DESC, RowNumber DESC) AS CurrentPersonIdentifier
  FROM ESAICompletePersonTable
),
MemberPersonBridge AS (
  SELECT 
     fp.UniqueRecord AS ESAIInternalPersonID,
     CASE WHEN cp.CurrentPersonIdentifier = 1 THEN 1 ELSE 0 END AS IsCurrent,
     cp.UniqueRecord, cp.FileLayoutID, cp.FileID, cp.RowNumber, cp.name_family, cp.name_given_first, cp.birthDate, 
     cp.gender, cp.address_permanent_line1, cp.telecom_phone_home, cp.identifier_planMemberID, cp.identifier_beneficiaryID, 
     cp.identifier_uniquepersonkey, cp.MatchID
  FROM ESAIPersonWithIdentifiers cp
  LEFT JOIN ESAIPersonWithIdentifiers fp 
    ON cp.MatchID = fp.MatchID AND fp.FirstPersonIdentifier = 1
),
MemberPersonBridge_CurrPlanMbr AS (
  SELECT 
     ESAIInternalPersonID, IsCurrent, UniqueRecord, FileLayoutID, FileID, RowNumber, name_family, name_given_first, 
     birthDate, gender, address_permanent_line1, telecom_phone_home, identifier_planMemberID, identifier_beneficiaryID,
     IFNULL(NULLIF(identifier_planMemberID, 'None'), '') AS PlanMemberIdModified,
     IFNULL(NULLIF(identifier_uniquepersonkey, 'None'), '') AS UniquePersonKeyModified,
     identifier_uniquepersonkey, MatchID,
     CASE WHEN IFNULL(identifier_planMemberID, 'None') = 'None' THEN NULL 
          WHEN ROW_NUMBER() OVER(PARTITION BY identifier_planMemberID ORDER BY COALESCE(FileID, 0) DESC, COALESCE(RowNumber, 0) DESC) = 1 THEN 1 
          ELSE 0 END AS IsCurrentPlanMemberID,
     CASE WHEN IFNULL(identifier_uniquepersonkey, 'None') = 'None' THEN NULL 
          WHEN ROW_NUMBER() OVER(PARTITION BY identifier_uniquepersonkey ORDER BY COALESCE(FileID, 0) DESC, COALESCE(RowNumber, 0) DESC) = 1 THEN 1 
          ELSE 0 END AS IsCurrentUniquePersonKey,
     CASE WHEN ESAIInternalPersonID = UniqueRecord THEN 1 ELSE 0 END AS IsOriginalMemberID 
  FROM MemberPersonBridge
),
PUModPop AS (
  SELECT *,
      CASE WHEN PlanMemberIdModified <> '' THEN 1 ELSE 0 END AS IsPlanMemberIdPopulated,
      CASE WHEN UniquePersonKeyModified <> '' THEN 1 ELSE 0 END AS IsUniquePersonKeyModifiedPopulated,
      CONCAT(PlanMemberIdModified, '-', UniquePersonKeyModified) AS PMUP
  FROM MemberPersonBridge_CurrPlanMbr
),
Final AS (
  SELECT *,
      CAST(COALESCE(IsCurrentPlanMemberID, IsCurrentUniquePersonKey, 1) AS INT) AS IsCurrentPMUPComputed
  FROM PUModPop
)
SELECT 
   ESAIInternalPersonID,
   IsCurrent,
   UniqueRecord,
   CAST(FileLayoutID AS INT) AS FileLayoutID,
   FileID,
   name_family,
   name_given_first,
   birthDate,
   gender,
   address_permanent_line1,
   telecom_phone_home,
   identifier_planMemberID,
   identifier_beneficiaryID,
   identifier_uniquepersonkey,
   sha2(concat_ws('|',
     IFNULL(ESAIInternalPersonID,''), IFNULL(CAST(IsCurrent AS STRING),''), IFNULL(UniqueRecord,''),
     IFNULL(CAST(FileLayoutID AS STRING),''), IFNULL(CAST(FileID AS STRING),''), IFNULL(name_family,''),
     IFNULL(name_given_first,''), IFNULL(birthDate,''), IFNULL(gender,''), IFNULL(address_permanent_line1,''),
     IFNULL(telecom_phone_home,''), IFNULL(identifier_planMemberID,''), IFNULL(identifier_beneficiaryID,''), IFNULL(identifier_uniquepersonkey,''),
     IFNULL(CAST(IsCurrentPlanMemberID AS STRING),''), IFNULL(CAST(IsCurrentUniquePersonKey AS STRING),''),
     IFNULL(CAST(IsOriginalMemberID AS STRING),''), IFNULL(PMUP,''), IFNULL(CAST(IsCurrentPMUPComputed AS STRING),'')
   ), 256) AS hashKey,
   IsCurrentPlanMemberID,
   IsCurrentUniquePersonKey,
   IsOriginalMemberID,
   PMUP,
   IsCurrentPMUPComputed AS IsCurrentPMUP
FROM Final
"""


def LoadSource(df_bronze):
    """Transforms raw bronze PySpark DataFrame into prepared feature dataset."""
    windowPartition = Window.partitionBy(col("FileID")).orderBy(col("FileID"))

    return df_bronze.withColumn("RecordHash", sha2(to_json(struct("*")), 256)) \
        .distinct() \
        .withColumn("RowNumber", row_number().over(windowPartition)) \
        .withColumn("UniqueRecord", concat_ws("-", col("FileID"), col("RowNumber"))) \
        .withColumn("FileID", col("FileID")) \
        .withColumn("FileLayoutID", col("FileLayoutID")) \
        .withColumn("name_family", upper(trim(col("name_family")))) \
        .withColumn("name_given_first", upper(trim(col("name_given_first")))) \
        .withColumn("name_family_initial", upper(substring(trim(col("name_family")), 1, 1))) \
        .withColumn("name_given_first_initial", upper(substring(trim(col("name_given_first")), 1, 1))) \
        .withColumn("birthDate", expr("try_cast(birthDate as string)")) \
        .withColumn("birthDate_formatted", expr("regexp_replace(try_cast(birthDate as string), '[^0-9]', '')")) \
        .withColumn("gender", upper(trim(col("gender")))) \
        .withColumn("address_permanent_line1", upper(trim(col("address_permanent_line1")))) \
        .withColumn("telecom_phone_home", trim(col("telecom_phone_home"))) \
        .withColumn("telecom_phone_home_formatted", regexp_replace(trim(col("telecom_phone_home")), "[^0-9]", "")) \
        .withColumn("identifier_planMemberID", upper(trim(col("identifier_planMemberID")))) \
        .withColumn("identifier_beneficiaryID", upper(trim(col("identifier_beneficiaryID")))) \
        .withColumn("identifier_uniquepersonkey", upper(trim(col("identifier_uniquepersonkey")))) \
        .select(
            "UniqueRecord", "FileLayoutID", "FileID", "RowNumber", "name_family", "name_given_first", 
            "gender", "telecom_phone_home", "address_permanent_line1", "birthDate", "birthDate_formatted", 
            "identifier_planMemberID", "identifier_beneficiaryID", "identifier_uniquepersonkey", 
            "name_family_initial", "name_given_first_initial", "telecom_phone_home_formatted"
        )


def RulesToCompare(rules, pandasMem_df):
    """Runs record linkage evaluation on pandas DataFrame against matching rules."""
    matchesAllRules_df = pd.DataFrame()
    for lst in rules:
        indexer = recordlinkage.Index()
        indexer.block(lst[0])
        candidatesBlock = indexer.index(pandasMem_df)
        
        compareBlock = recordlinkage.Compare()
        threshold = lst[-1]
        for col_name in CompareColumns:
            compareBlock.exact(col_name, col_name, label=str(col_name))
            
        features = compareBlock.compute(candidatesBlock, pandasMem_df)
        features[lst[0]] = features[lst[0]].apply(lambda x: x * 10)
        
        matchesRule = features[features[lst[:-1]].sum(axis=1) >= threshold]
        matchesRule_df = matchesRule.index.to_frame()
        
        if len(matchesRule_df) > 0:
            matchesAllRules_df = pd.concat([matchesAllRules_df, matchesRule_df])
            
    return matchesAllRules_df


def RunLinking(sparkMem_df, spark_session=None):
    """Executes the linkage workflow and outputs an updated PySpark DataFrame."""
    spark = spark_session or SparkSession.getActiveSession()
    pandasMem_df = sparkMem_df.toPandas()
    
    if pandasMem_df["UniqueRecord"].duplicated().any():
        pandasMem_df = pandasMem_df.reset_index(drop=True)
        pandasMem_df["UniqueRecord"] = pandasMem_df.index.astype(str) + "_" + pandasMem_df["UniqueRecord"].astype(str)
    
    pandasMem_df = pandasMem_df.set_index("UniqueRecord", drop=False)
    
    dfCombined = RulesToCompare(RulesAll, pandasMem_df)
    
    def convert_to_spark_df(pdf):
        """Converts Pandas DF to Spark DF with explicit safe schemas."""
        pdf_clean = pdf.copy()
        for column in pdf_clean.columns:
            if pdf_clean[column].apply(lambda x: isinstance(x, list)).any():
                pdf_clean[column] = pdf_clean[column].apply(lambda x: ",".join(map(str, x)) if isinstance(x, list) else str(x))
            else:
                pdf_clean[column] = pdf_clean[column].astype(str)
        
        schema_fields = []
        for c in pdf_clean.columns:
            if c in ["FileID", "RowNumber"]:
                schema_fields.append(StructField(c, LongType(), True))
                pdf_clean[c] = pd.to_numeric(pdf_clean[c], errors='coerce').fillna(0).astype('int64')
            elif c == "FileLayoutID":
                schema_fields.append(StructField(c, IntegerType(), True))
                pdf_clean[c] = pd.to_numeric(pdf_clean[c], errors='coerce').fillna(0).astype('int32')
            else:
                schema_fields.append(StructField(c, StringType(), True))
                
        schema = StructType(schema_fields)
        return spark.createDataFrame(pdf_clean, schema=schema)

    if dfCombined.empty:
        finalPandas_df = pandasMem_df.copy()
        finalPandas_df['MatchID'] = finalPandas_df['UniqueRecord']
        return convert_to_spark_df(finalPandas_df)
    
    dfCombined.columns = [0, 1]
    
    dfMatchedColumnA = dfCombined.rename(columns={0: "A", 1: "B"})
    dfMatchedColumnB = dfCombined.rename(columns={0: "B", 1: "A"})
    
    dfMatched = pd.concat([dfMatchedColumnA, dfMatchedColumnB], ignore_index=True)
    dfMatched.drop_duplicates(inplace=True)
    
    matchesAll_df = pd.concat([
        dfMatched[["A", "B"]].rename(columns={"A": "Record", "B": "Match"}),
        dfMatched[["B", "A"]].rename(columns={"B": "Record", "A": "Match"})
    ]).reset_index(drop=True)
    
    matchesSame_df = matchesAll_df[["Record", "Record"]].copy()
    matchesSame_df.columns = ['Record', 'Match']
    
    matchesAll_df = pd.concat([matchesAll_df, matchesSame_df], ignore_index=True)
    matchesAll_df.drop_duplicates(inplace=True)
    
    distinctRow = dfMatched.groupby('A').head(1).drop('B', axis=1)
    
    matchesAll_df = pd.merge(matchesAll_df, distinctRow, how="left", left_on="Record", right_on="A").drop('A', axis=1)
    
    matchesAll_df = pd.merge(matchesAll_df, pandasMem_df["UniqueRecord"], left_on="Record", right_index=True).rename(columns={"UniqueRecord": "RecordID"})
    matchesAll_df = pd.merge(matchesAll_df, pandasMem_df["UniqueRecord"], left_on="Match", right_index=True).rename(columns={"UniqueRecord": "MatchID"})
    
    matched_df = matchesAll_df.groupby("RecordID") \
            .agg({"MatchID": lambda x: ",".join(sorted(pd.unique(x)))}) \
            .reset_index()
    
    matchesAllModified = matchesAll_df.groupby("RecordID").head(1)
    drop_cols = ['MatchID', 'Match', 'Record', 'index']
    existing_drops = [c for c in drop_cols if c in matchesAllModified.columns]
    matchesAllModified = matchesAllModified.drop(columns=existing_drops)
        
    newDFToMatch = pd.merge(matched_df, matchesAllModified, how="left", on="RecordID")
    
    finalPandas_df = pandasMem_df.reset_index(drop=True).merge(newDFToMatch, left_on="UniqueRecord", right_on="RecordID", how="left")
    finalPandas_df['MatchID'] = finalPandas_df['MatchID'].fillna(finalPandas_df['UniqueRecord'])
    
    return convert_to_spark_df(finalPandas_df)


def ProcessMemberBridge(df_bronze, spark_session=None):
    """Main function called inside your DLT notebook pipeline to process streaming data."""
    spark = spark_session or SparkSession.getActiveSession()
    
    sparkMem_df = LoadSource(df_bronze)
    numRows = sparkMem_df.count()

    if numRows == 0:
        return spark.createDataFrame([], sparkMem_df.schema)
    elif numRows == 1:
        convertedSpark_df = sparkMem_df.withColumn("MatchID", col("UniqueRecord"))
    else:
        convertedSpark_df = RunLinking(sparkMem_df, spark)

    convertedSpark_df.createOrReplaceTempView("ESAICompletePersonTable")
    
    result_df = spark.sql(FINAL_SQL)
    
    return result_df
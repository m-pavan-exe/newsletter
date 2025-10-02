"""SQL queries for the BS Summary pipeline."""

# Customer balance analysis
CUSTOMER_BALANCE_ANALYSIS = """
SELECT DISTINCT 
    Dm_Mis_Summary.COUNTRY AS Country,
    Dm_Mis_Summary.LE_BOOK AS Le_Book,
    Dm_Mis_Summary.Customer_ID,
    Dm_Mis_Summary.Customer_Name,
    Dm_Mis_Summary.Account_Officer,
    Dm_Mis_Summary.AO_Name,
    Mgt_Expanded.MGT_Line AS Mgt_Line,
    Mgt_Expanded.MGT_Line_Description AS Mgt_Line_Description,
    Mgt_Expanded.MGT_Line_Level AS Mgt_Line_Level,
    Abs(SUM(Dm_Mis_Summary.BUSINESS_DAY)) AS Business_Day,
    Abs(SUM(Dm_Mis_Summary.PREVIOUS_DAY)) AS Previous_Day
FROM Dm_Mis_Details Dm_Mis_Summary
INNER JOIN Mgt_Expanded Mgt_Expanded
    ON (Dm_Mis_Summary.MRL_LINE = Mgt_Expanded.SOURCE_MRL_LINE
        AND Dm_Mis_Summary.BAL_TYPE = Mgt_Expanded.SOURCE_BAL_TYPE)
WHERE 
    Dm_Mis_Summary.COUNTRY = :country
    AND Dm_Mis_Summary.LE_BOOK = :le_book
    AND Mgt_Expanded.MGT_Line IN :mgt_line
    AND Mgt_Expanded.MGT_Line_Level = :mgt_line_level
GROUP BY 
    Dm_Mis_Summary.COUNTRY,
    Dm_Mis_Summary.LE_BOOK,
    Dm_Mis_Summary.Customer_ID,
    Dm_Mis_Summary.Customer_Name,
    Dm_Mis_Summary.Account_Officer,
    Dm_Mis_Summary.AO_Name,
    Mgt_Expanded.MGT_Line,
    Mgt_Expanded.MGT_Line_Description,
    Mgt_Expanded.MGT_Line_Level
ORDER BY 
    Mgt_Line ASC
"""

# Data retrieval queries
GET_OVERALL_AMOUNTS = """
SELECT 
    Mgt_Line_Description,
    SUM(Today) AS today,
    SUM(Yesterday) AS yesterday,
    (SUM(Today) - SUM(Yesterday)) / NULLIF(SUM(Yesterday), 0) * 100 AS percentage_change
FROM Tmp_DS 
WHERE Mgt_Line_Level = :mgmt_line_level 
  AND Mgt_Line = :mgmt_line
  AND Bal_Type = :bal_type
GROUP BY Mgt_Line_Description
HAVING SUM(Yesterday) != 0
"""

GET_CATALOG_REPORT="""
SELECT DISTINCT 
    Dm_Mis_Summary.COUNTRY AS Country,
    Dm_Mis_Summary.LE_BOOK AS Le_Book,
    Mgt_Expanded.MGT_Line AS Mgt_Line,
    Mgt_Expanded.MGT_Line_Description AS Mgt_Line_Description,
    Mgt_Expanded.MGT_Line_Level AS Mgt_Line_Level,
    Abs(SUM(Dm_Mis_Summary.BUSINESS_DAY)) AS Business_Day,
    Abs(SUM(Dm_Mis_Summary.PREVIOUS_DAY)) AS Previous_Day,
    Abs(SUM(Dm_Mis_Summary.YTD)) AS Ytd,
    Abs(SUM(Dm_Mis_Summary.YTD_TARGET)) AS Ytd_Target,
    CASE 
        WHEN SUM(Dm_Mis_Summary.PREVIOUS_DAY) = 0 THEN 100
        ELSE ROUND(
            (SUM(Dm_Mis_Summary.BUSINESS_DAY) - SUM(Dm_Mis_Summary.PREVIOUS_DAY)) 
            / NULLIF(SUM(Dm_Mis_Summary.PREVIOUS_DAY), 0) * 100, 
        2)
    END AS Today_vs_Yesterday,
    CASE 
        WHEN SUM(Dm_Mis_Summary.YTD_TARGET) = 0 THEN 100
        ELSE ROUND(
            (SUM(Dm_Mis_Summary.YTD) - SUM(Dm_Mis_Summary.YTD_TARGET)) 
            / NULLIF(SUM(Dm_Mis_Summary.YTD_TARGET), 0) * 100, 
        2)
    END AS Actual_vs_Target
    FROM Dm_Mis_Summary Dm_Mis_Summary
INNER JOIN Mgt_Expanded Mgt_Expanded
    ON (DM_MIS_SUMMARY.MRL_LINE = MGT_EXPANDED.SOURCE_MRL_LINE
        AND DM_MIS_SUMMARY.BAL_TYPE = MGT_EXPANDED.SOURCE_BAL_TYPE)
WHERE 
    Dm_Mis_Summary.COUNTRY = 'KE'
    AND Dm_Mis_Summary.LE_BOOK = '01'
    AND Mgt_Expanded.MGT_Line IN ('G011020','G011030','G011100','G011999','G012010','G012020','G012999','G015999','G016999','G017049','G017999')
    AND Mgt_Expanded.MGT_Line_Level = '1'
GROUP BY 
    Dm_Mis_Summary.COUNTRY,
    Dm_Mis_Summary.LE_BOOK,
    Mgt_Expanded.MGT_Line,
    Mgt_Expanded.MGT_Line_Description,
    Mgt_Expanded.MGT_Line_Level
ORDER BY 
    Mgt_Line ASC
"""

GET_CATALOG_REPORT1 = """
SELECT  
   Case when Mgt_Line in ('G011999','G013149') then 'Total Assets' 
    when Mgt_Line in ('G012999','G013199') then 'Total Liability' End 
    Mgt_Line_Description,
   t2.Source_Mgt_Line,
   (SELECT Mgt_Line_Description 
    FROM Mgt_Lines 
    WHERE Mgt_Line = t2.Source_Mgt_Line) AS Source_mgt_Line_Desc,
    t1.Mrl_Line,t1.Mrl_Description,
    t1.Customer_Id,t1.Customer_Name,t1.Account_Officer, t1.Ao_NAme,
   t1.BAL_TYPE AS Bal_Type,
   case when Mgt_Line in ('G011999','G013149') then SUM(t1.BUSINESS_DAY) * -1 else SUM(t1.BUSINESS_DAY) End Business_day,
   case when Mgt_Line in ('G011999','G013149') then SUM(t1.PREVIOUS_DAY) * -1 else SUM(t1.PREVIOUS_DAY) End Previous_Day,
   t1.COUNTRY AS Country,
   t1.LE_BOOK AS Le_Book
  
FROM    
   DM_Mis_Details t1
INNER JOIN
   Mgt_Expanded t2
ON 
   t1.MRL_LINE = t2.SOURCE_MRL_LINE
   AND t1.BAL_TYPE = t2.SOURCE_BAL_TYPE
WHERE     
   (t2.MGT_Line IN ('G011999', 'G012999','G013149','G013199') AND T1.BAL_TYPE = 1)   AND t1.COUNTRY = 'KE'
   AND t1.LE_BOOK = '01'
   AND t1.CCY_TYPE = 'BCY'
GROUP BY 
    t2.MGT_Line,
    t2.MGT_Line_Description,
    t1.Mrl_Line,t1.Mrl_Description,
    t2.Source_Mgt_Line,
    t1.BAL_TYPE,
    t1.COUNTRY,
   t1.LE_BOOK,t1.Customer_Id,t1.Customer_Name,t1.Account_Officer, t1.Ao_NAme"""


GET_CATALOG_REPORT2 = """
SELECT
    t1.Mrl_Line,t1.Mrl_Description,
   t2.MGT_Line AS Mgt_Line,
   Case when Mgt_Line in ('G011999','G013149') then 'Total Income' 
    when Mgt_Line in ('G012999','G013199') then 'Total Expense' 
    when Mgt_Line in ('G015999') then 'Total Income'
    when Mgt_Line in ('G017049') then 'Total Expense'
    End 
    Mgt_Line_Description,
   t2.Source_Mgt_Line,
   (SELECT Mgt_Line_Description
    FROM Mgt_Lines
    WHERE Mgt_Line = t2.Source_Mgt_Line) AS Source_mgt_Line_Desc,
    Case when Mgt_Line in ('G012999','G013199','G017049') then SUM(t1.MTD) * -1 else SUM(t1.MTD) End MTD,
   Case when Mgt_Line in ('G012999','G013199','G017049') then SUM(t1.BUSINESS_DAY) * -1 else SUM(t1.BUSINESS_DAY) End Business_day,
   Case when Mgt_Line in ('G012999','G013199','G017049') then SUM(t1.Previous_Day) * -1 else SUM(t1.Previous_Day) End Previous_Day
FROM    
   DM_Mis_Details t1
INNER JOIN
   Mgt_Expanded t2
ON
   t1.MRL_LINE = t2.SOURCE_MRL_LINE
   AND t1.BAL_TYPE = t2.SOURCE_BAL_TYPE
WHERE     
   (t2.MGT_Line IN ('G011999', 'G013149','G012999','G013199','G015999','G017049') AND T1.BAL_TYPE = 3)
   and t2.Mgt_Line_Level = 1 
   AND t1.COUNTRY = 'KE'
   AND t1.LE_BOOK = '01'
   AND t1.CCY_TYPE = 'BCY'
GROUP BY
   t2.MGT_Line,
   t2.MGT_Line_Description,
   t1.Mrl_Line,t1.Mrl_Description,
   t2.Source_Mgt_Line,Mgt_Line_Description
Order by MGT_Line"""
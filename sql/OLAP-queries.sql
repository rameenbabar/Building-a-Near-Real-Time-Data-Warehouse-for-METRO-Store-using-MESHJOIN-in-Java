use metro_dw;

-- Q1
SELECT 
    MONTH(T.Date) AS Month,
    P.Product_Name,
    SUM(S.Total_Sale) AS Total_Revenue,
    T.Is_Weekend
FROM 
    SALES S
JOIN 
    TIME_DIM T ON S.Date_ID = T.Date_ID
JOIN 
    PRODUCT P ON S.Product_ID = P.Product_ID
WHERE 
    T.Year = 2019 
GROUP BY 
    MONTH(T.Date), P.Product_Name, T.Is_Weekend
ORDER BY 
    MONTH(T.Date) ASC, T.Is_Weekend ASC, Total_Revenue DESC
LIMIT 5;

-- Q2
WITH Quarterly_Revenue AS (
    SELECT
        SALES.Store_ID,
        STORE.Store_Name,
        QUARTER(TIME_DIM.Date) AS Quarter,
        SUM(SALES.Total_Sale) AS Total_Revenue
    FROM
        SALES
    JOIN
        TIME_DIM ON SALES.Date_ID = TIME_DIM.Date_ID
    JOIN
        STORE ON SALES.Store_ID = STORE.Store_ID
    WHERE
        TIME_DIM.Year = 2019
    GROUP BY
        SALES.Store_ID, STORE.Store_Name, QUARTER(TIME_DIM.Date)
)
SELECT
    Store_Name,
    CONCAT('Q', Quarter) AS Quarter,
    Total_Revenue,
    ROUND(
        (
            Total_Revenue - LAG(Total_Revenue) OVER (PARTITION BY Store_Name ORDER BY Quarter)
        ) / LAG(Total_Revenue) OVER (PARTITION BY Store_Name ORDER BY Quarter) * 100,
        2
    ) AS Growth_Rate
FROM
    Quarterly_Revenue
ORDER BY
    Store_Name, Quarter;

-- Q3
SELECT
    ST.Store_Name,
    SU.Supplier_Name,
    P.Product_Name,
    SUM(S.Total_Sale) AS Total_Contribution
FROM
    SALES S
JOIN
    STORE ST ON S.Store_ID = ST.Store_ID
JOIN
    SUPPLIER SU ON S.Supplier_ID = SU.Supplier_ID
JOIN
    PRODUCT P ON S.Product_ID = P.Product_ID
GROUP BY
    ST.Store_Name, SU.Supplier_Name, P.Product_Name
ORDER BY
    ST.Store_Name ASC, SU.Supplier_Name ASC, Total_Contribution DESC;
    
-- Q4
WITH SeasonalMapping AS (
    SELECT
        Date_ID,
        CASE 
            WHEN MONTH(Date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(Date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(Date) IN (9, 10, 11) THEN 'Fall'
            WHEN MONTH(Date) IN (12, 1, 2) THEN 'Winter'
        END AS Season
    FROM
        TIME_DIM
),
SeasonalSales AS (
    SELECT
        S.Product_ID,
        P.Product_Name,
        SM.Season,
        SUM(S.Total_Sale) AS Total_Seasonal_Sales
    FROM
        SALES S
    JOIN
        SeasonalMapping SM ON S.Date_ID = SM.Date_ID
    JOIN
        PRODUCT P ON S.Product_ID = P.Product_ID
    GROUP BY
        S.Product_ID, P.Product_Name, SM.Season
)
SELECT
    Product_Name,
    Season,
    Total_Seasonal_Sales
FROM
    SeasonalSales
ORDER BY
    Product_Name, 
    CASE 
        WHEN Season = 'Spring' THEN 1
        WHEN Season = 'Summer' THEN 2
        WHEN Season = 'Fall' THEN 3
        WHEN Season = 'Winter' THEN 4
    END;

-- Q5
WITH MonthlyRevenue AS (
    SELECT
        S.Store_ID,
        ST.Store_Name,
        S.Supplier_ID,
        SU.Supplier_Name,
        MONTH(T.Date) AS Month,
        YEAR(T.Date) AS Year,
        SUM(S.Total_Sale) AS Total_Monthly_Revenue
    FROM
        SALES S
    JOIN
        STORE ST ON S.Store_ID = ST.Store_ID
    JOIN
        SUPPLIER SU ON S.Supplier_ID = SU.Supplier_ID
    JOIN
        TIME_DIM T ON S.Date_ID = T.Date_ID
    GROUP BY
        S.Store_ID, ST.Store_Name, S.Supplier_ID, SU.Supplier_Name, YEAR(T.Date), MONTH(T.Date)
),
RevenueVolatility AS (
    SELECT
        MR1.Store_ID,
        MR1.Store_Name,
        MR1.Supplier_ID,
        MR1.Supplier_Name,
        MR1.Year,
        MR1.Month AS Current_Month,
        MR1.Total_Monthly_Revenue AS Current_Revenue,
        MR2.Total_Monthly_Revenue AS Previous_Revenue,
        CASE 
            WHEN MR2.Total_Monthly_Revenue IS NULL OR MR2.Total_Monthly_Revenue = 0 THEN NULL
            ELSE ((MR1.Total_Monthly_Revenue - MR2.Total_Monthly_Revenue) / MR2.Total_Monthly_Revenue) * 100
        END AS Revenue_Volatility
    FROM
        MonthlyRevenue MR1
    LEFT JOIN
        MonthlyRevenue MR2
    ON
        MR1.Store_ID = MR2.Store_ID
        AND MR1.Supplier_ID = MR2.Supplier_ID
        AND MR1.Year = MR2.Year
        AND MR1.Month = MR2.Month + 1 -- Previous month
)
SELECT
    Store_Name,
    Supplier_Name,
    Current_Month,
    Current_Revenue,
    Previous_Revenue,
    Revenue_Volatility
FROM
    RevenueVolatility
ORDER BY
    Store_Name, Supplier_Name, Current_Month;

-- Q6
WITH ProductPairs AS (
    SELECT
        A.Product_ID AS Product1_ID,
        B.Product_ID AS Product2_ID,
        COUNT(*) AS Frequency
    FROM
        SALES A
    JOIN
        SALES B
    ON
        A.Transaction_ID = B.Transaction_ID
        AND A.Product_ID < B.Product_ID -- Avoid duplicate pairs and self-pairs
    GROUP BY
        A.Product_ID, B.Product_ID
),
TopPairs AS (
    SELECT
        PP.Product1_ID,
        P1.Product_Name AS Product1_Name,
        PP.Product2_ID,
        P2.Product_Name AS Product2_Name,
        PP.Frequency
    FROM
        ProductPairs PP
    JOIN
        PRODUCT P1 ON PP.Product1_ID = P1.Product_ID
    JOIN
        PRODUCT P2 ON PP.Product2_ID = P2.Product_ID
    ORDER BY
        PP.Frequency DESC
    LIMIT 5
)
SELECT
    Product1_Name,
    Product2_Name,
    Frequency
FROM
    TopPairs;

-- Q7
SELECT
    ST.Store_Name,
    SU.Supplier_Name,
    P.Product_Name,
    T.Year,
    SUM(S.Total_Sale) AS Yearly_Revenue
FROM
    SALES S
JOIN
    STORE ST ON S.Store_ID = ST.Store_ID
JOIN
    SUPPLIER SU ON S.Supplier_ID = SU.Supplier_ID
JOIN
    PRODUCT P ON S.Product_ID = P.Product_ID
JOIN
    TIME_DIM T ON S.Date_ID = T.Date_ID
GROUP BY
    ROLLUP (ST.Store_Name, SU.Supplier_Name, P.Product_Name, T.Year)
ORDER BY
    ST.Store_Name ASC,
    SU.Supplier_Name ASC,
    P.Product_Name ASC,
    T.Year ASC;

-- Q8
WITH ProductSales AS (
    SELECT
        P.Product_ID,
        P.Product_Name,
        CASE 
            WHEN MONTH(T.Date) BETWEEN 1 AND 6 THEN 'H1'
            WHEN MONTH(T.Date) BETWEEN 7 AND 12 THEN 'H2'
        END AS Half,
        SUM(S.Total_Sale) AS Total_Revenue,
        SUM(S.Quantity) AS Total_Quantity
    FROM
        SALES S
    JOIN
        TIME_DIM T ON S.Date_ID = T.Date_ID
    JOIN
        PRODUCT P ON S.Product_ID = P.Product_ID
    GROUP BY
        P.Product_ID, P.Product_Name, Half
),
YearlyTotals AS (
    SELECT
        P.Product_ID,
        P.Product_Name,
        SUM(S.Total_Sale) AS Yearly_Revenue,
        SUM(S.Quantity) AS Yearly_Quantity
    FROM
        SALES S
    JOIN
        TIME_DIM T ON S.Date_ID = T.Date_ID
    JOIN
        PRODUCT P ON S.Product_ID = P.Product_ID
    GROUP BY
        P.Product_ID, P.Product_Name
)
SELECT
    PS.Product_Name,
    PS.Half,
    PS.Total_Revenue AS Revenue_Per_Half,
    PS.Total_Quantity AS Quantity_Per_Half,
    YT.Yearly_Revenue,
    YT.Yearly_Quantity
FROM
    ProductSales PS
JOIN
    YearlyTotals YT ON PS.Product_ID = YT.Product_ID
ORDER BY
    PS.Product_Name, PS.Half;
    
-- Q9
WITH DailySales AS (
    SELECT
        P.Product_ID,
        P.Product_Name,
        T.Date,
        SUM(S.Total_Sale) AS Daily_Revenue
    FROM
        SALES S
    JOIN
        TIME_DIM T ON S.Date_ID = T.Date_ID
    JOIN
        PRODUCT P ON S.Product_ID = P.Product_ID
    GROUP BY
        P.Product_ID, P.Product_Name, T.Date
),
AverageSales AS (
    SELECT
        DS.Product_ID,
        DS.Product_Name,
        AVG(DS.Daily_Revenue) AS Daily_Average_Revenue
    FROM
        DailySales DS
    GROUP BY
        DS.Product_ID, DS.Product_Name
),
Outliers AS (
    SELECT
        DS.Product_Name,
        DS.Date,
        DS.Daily_Revenue,
        ASales.Daily_Average_Revenue,
        CASE
            WHEN DS.Daily_Revenue > 2 * ASales.Daily_Average_Revenue THEN 'Outlier'
            ELSE 'Normal'
        END AS Spike_Flag
    FROM
        DailySales DS
    JOIN
        AverageSales ASales ON DS.Product_ID = ASales.Product_ID
)
SELECT
    Product_Name,
    Date,
    Daily_Revenue,
    Daily_Average_Revenue,
    Spike_Flag
FROM
    Outliers
WHERE
    Spike_Flag = 'Outlier'
ORDER BY
    Product_Name, Date;

-- Q10
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT
    ST.Store_Name,
    T.Year,
    QUARTER(T.Date) AS Quarter,
    SUM(S.Total_Sale) AS Total_Quarterly_Sales
FROM
    SALES S
JOIN
    STORE ST ON S.Store_ID = ST.Store_ID
JOIN
    TIME_DIM T ON S.Date_ID = T.Date_ID
GROUP BY
    ST.Store_Name, T.Year, QUARTER(T.Date)
ORDER BY
    ST.Store_Name ASC, T.Year ASC, Quarter ASC;

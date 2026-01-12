-- Sample SQL query for extracting daily trades
-- Parameters: @StartDate, @EndDate (SQL Server) or :StartDate, :EndDate (Oracle)

SELECT 
    TradeId,
    TradeDate,
    Symbol,
    Quantity,
    Price,
    TotalAmount,
    TradeType,
    AccountId,
    CreatedAt
FROM dbo.Trades
WHERE TradeDate BETWEEN @StartDate AND @EndDate
ORDER BY TradeDate DESC, TradeId

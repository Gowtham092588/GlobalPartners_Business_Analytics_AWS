CREATE TRIGGER [dbo].[trg_order_items_updated_at]
ON [globalpartnersDB].[dbo].[order_items]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE oi
    SET updated_at = SYSUTCDATETIME()
    FROM globalpartnersDB.dbo.order_items oi
    INNER JOIN inserted i
        ON oi.ORDER_ID = i.ORDER_ID
       AND oi.LINEITEM_ID = i.LINEITEM_ID;
END;
GO

ALTER TABLE [dbo].[order_items] ENABLE TRIGGER [trg_order_items_updated_at]
GO

CREATE TRIGGER [dbo].[trg_order_item_options_updated_at]
ON [globalpartnersDB].[dbo].[order_item_options]
AFTER UPDATE
AS
BEGIN 
    SET NOCOUNT ON;
    
    UPDATE oip
    SET updated_at = SYSUTCDATETIME()
    FROM globalpartnersDB.dbo.order_item_options oip
    INNER JOIN inserted i
        ON oip.ORDER_ID = i.ORDER_ID
        AND oip.OPTION_ID = i.OPTION_ID;

END;
GO

ALTER TABLE [dbo].[order_item_options] ENABLE TRIGGER [trg_order_item_options_updated_at]
GO

CREATE TRIGGER [dbo].[trg_date_dim_updated_at]
ON [globalpartnersDB].[dbo].[date_dim] 
AFTER UPDATE
AS 
BEGIN
    SET NOCOUNT ON;

    UPDATE dd
    SET updated_at = SYSUTCDATETIME()
    FROM globalpartnersDB.dbo.date_dim dd
    INNER JOIN inserted i
        ON dd.DATE_KEY = i.DATE_KEY;
END;
GO

ALTER TABLE [dbo].[date_dim] ENABLE TRIGGER [trg_date_dim_updated_at]
GO





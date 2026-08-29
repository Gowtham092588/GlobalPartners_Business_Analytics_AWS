ALTER TABLE [dbo].[order_items] ADD  CONSTRAINT [DF_order_items_updated_at]  DEFAULT (sysutcdatetime()) FOR [updated_at]
GO


ALTER TABLE [dbo].[order_item_options] ADD  CONSTRAINT [DF_order_item_options_updated_at]  DEFAULT (sysutcdatetime()) FOR [updated_at]
GO

ALTER TABLE [dbo].[date_dim] ADD  CONSTRAINT [DF_date_dim_updated_at]  DEFAULT (sysutcdatetime()) FOR [updated_at]
GO

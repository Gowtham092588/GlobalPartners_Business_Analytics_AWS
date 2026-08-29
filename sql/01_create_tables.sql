CREATE TABLE [dbo].[order_items](
	[APP_NAME] [nvarchar](50) NOT NULL,
	[RESTAURANT_ID] [nvarchar](50) NOT NULL,
	[CREATION_TIME_UTC] [nvarchar](50) NOT NULL,
	[ORDER_ID] [varchar](50) NOT NULL,
	[USER_ID] [nvarchar](50) NULL,
	[PRINTED_CARD_NUMBER] [nvarchar](50) NULL,
	[IS_LOYALTY] [bit] NOT NULL,
	[CURRENCY] [nvarchar](50) NOT NULL,
	[LINEITEM_ID] [nvarchar](50) NULL,
	[ITEM_CATEGORY] [nvarchar](100) NULL,
	[ITEM_NAME] [nvarchar](100) NULL,
	[ITEM_PRICE] [decimal](10, 2) NOT NULL,
	[ITEM_QUANTITY] [int] NOT NULL,
	[updated_at] [datetime2](7) NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[order_item_options](
	[ORDER_ID] [nvarchar](50) NOT NULL,
	[LINEITEM_ID] [nvarchar](50) NOT NULL,
	[OPTION_GROUP_NAME] [nvarchar](100) NOT NULL,
	[OPTION_NAME] [nvarchar](100) NOT NULL,
	[OPTION_PRICE] [decimal](10, 2) NOT NULL,
	[OPTION_QUANTITY] [int] NOT NULL,
	[updated_at] [datetime2](7) NULL,
	[option_id] [bigint] IDENTITY(1,1) NOT NULL,
 CONSTRAINT [UQ_order_item_options_option_id] UNIQUE NONCLUSTERED 
(
	[option_id] ASC
)
GO

CREATE TABLE [dbo].[date_dim](
	[date_key] [date] NOT NULL,
	[year] [smallint] NOT NULL,
	[month] [tinyint] NOT NULL,
	[week] [tinyint] NOT NULL,
	[day_of_week] [nvarchar](50) NOT NULL,
	[is_weekend] [bit] NOT NULL,
	[is_holiday] [bit] NOT NULL,
	[holiday_name] [nvarchar](50) NULL,
	[updated_at] [datetime2](7) NULL
) ON [PRIMARY]
GO


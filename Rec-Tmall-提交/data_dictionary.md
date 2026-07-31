# Rec-Tmall 数据字典

> 生成时间: 2026-07-30 02:20:45

## 商品属性表 (product)

行数: 8,133,507

| 字段 | 类型 | 说明 | 缺失率 |
|------|------|------|--------|
| item_id | int | 商品ID，唯一标识 | 0.00% |
| title | string | NLP提取的关键词，空格分隔 | 0.00% |
| pict_url | string | 商品图片URL | 0.00% |
| category | string | 类别，格式x-y | 0.00% |
| brand_id | string | 品牌ID，如b1 | 24.60% |
| seller_id | string | 商家ID，如s1 | 0.00% |

## 用户行为日志表 (log)

行数: 1,259,003,848

| 字段 | 类型 | 说明 | 缺失率 |
|------|------|------|--------|
| item_id | int | 商品ID | 0.00% |
| user_id | string | 用户ID，如u9774184 | 0.00% |
| action | string | 行为类型: click/collect/cart/alipay | 0.00% |
| vtime | string | 行为时间，格式yyyy-mm-dd hh:mm:ss | 0.00% |
| action_date | date | 行为日期（分区字段，从vtime解析） | - |
| action_type | string | 行为类型（分区字段，同action） | - |

## 用户评论表 (review)

行数: 11,201,322

| 字段 | 类型 | 说明 | 缺失率 |
|------|------|------|--------|
| item_id | int | 商品ID | 0.00% |
| user_id | string | 用户ID | 0.00% |
| feedback | string | NLP提取的评论关键词，空格分隔 | 0.00% |


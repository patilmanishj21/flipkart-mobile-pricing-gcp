INSERT INTO `data-pipeline-dev-372513.flipkart.mobile_prices_trust`
SELECT
  trim(Brand),
  trim(MODEL),
  trim(Color),
  trim(Memory),
  trim(Storage),
  coalesce(Rating,0),
  coalesce(Selling_Price,0),
  coalesce(Original_Price,0)
FROM
  `data-pipeline-dev-372513.flipkart.mobile_prices`
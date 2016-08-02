# Weekly Reports 

## Week 3

- Added new aggregation functions: minf, maxf (sumf is already existing)
- Now dataframe element was split into: now_sec(unix timestamp in seconds) and now(unix timestamp in days). Both elements are allowed as grouping keys
- Changed acquisition_era parsing:
	- Before: from pattern /A/B-C-D/E acuisitinion_era was parsed as B-C and processing_era as D
	- Now: from pattern /A/B-C-D/E acuisitinion_era is parsed as B and processing_era as C-D
- Performance isues testing.

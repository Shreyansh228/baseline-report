
Problem Statement –
Need to generate report from International Baseline data of world versus U.S. with % -
1. E.g. for barley, find how much world can harvest from 2016/17 – 2028/29 vs % from US e.g. (“USA harvest in year” / “World harvest in year”) x 100
2. Final report format would be - Year|world_barley_harvest|usa_barley_contribution%|world_beef_slaughter|...|usa_wheat_ contribution%


For simplicity, I have divided input csv file into directories like  country -> product -> productDetails[including year]
with country folder as parent dir.

I have considered three products such as [barley, beef and cotton].
I have compared usa data with world data present in above mentioned hierarchy of folders.

I have divided the application into Reader Processor and Reporter interfaces where I have implmented concrete classes w.r.t plain Scala solution
approach and Spark solution approach.
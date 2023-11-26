filer.csv :
	scrapy crawl filers -L 'WARNING' -O $@

filing.jl :
	scrapy crawl filings -L 'WARNING' -O $@

organizations.csv :
	scrapy crawl organizations -L 'WARNING' -O $@

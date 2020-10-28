from scrapy import cmdline

#Uncomment the desired program to run
#cmdline.execute("scrapy crawl soccerwiki -o soccer.json".split())
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

settings = get_project_settings()
settings['FEED_FORMAT'] = 'json'
settings['LOG_LEVEL'] = 'INFO'
settings['FEED_URI'] = 'soccer_greece_div2_3.json'
settings['LOG_FILE'] = 'soccer.log'

process = CrawlerProcess(settings)


process.crawl('soccerwiki', 'https://en.soccerwiki.org/wiki.php?action=countryProfile&countryId=GRE', 'en.soccerwiki.org', 2)
process.crawl('soccerwiki', 'https://en.soccerwiki.org/wiki.php?action=countryProfile&countryId=GRE', 'en.soccerwiki.org', 3)
process.start()

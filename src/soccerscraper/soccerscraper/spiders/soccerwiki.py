# -*- coding: utf-8 -*-
import scrapy
from ..items import PlayerItem

class SoccerwikiSpider(scrapy.Spider):
    BASE_URL = "https://en.soccerwiki.org/"
    name = 'soccerwiki'
    allowed_domains = ['en.soccerwiki.org']
    #start_urls = ['https://en.soccerwiki.org/player.php?pid=84264']
    start_urls = ['https://en.soccerwiki.org/squad.php?clubid=372']

    def __init__(self, starturl, allowed, *args, **kwargs):
        print("init")

        self.start_urls = [starturl]
        self.allowed_domains = [allowed]
        self.division = args[0]
        print(self.start_urls)
        super().__init__((), **kwargs)

    def parse(self, response):
        print("parsing")
        print(self.division)

        next_page  = self.start_urls[0]
        if next_page is not None:
            print(next_page)
            request = scrapy.Request(next_page , callback=self.parse_country)
            request.meta['division'] = self.division
            yield request


    def parse_country(self, response):
        #get arguments
        division = response.meta.get('division')

        divisions = response.xpath("//*[@class = 'RcontentBox floatright']//*[@class = 'InnerBorder']")[0]

        #there is no such division for this country
        if(division > len(divisions.xpath('table'))):
            return
        else:
            divisionn = divisions.xpath('table')[division-1]
            teams = divisionn.xpath('tr//td//tr')
            for team in teams:
                team_url = self.BASE_URL+team.xpath('td//a//@href').extract()[0]
                if team_url is not None:
                    print(team_url)
                    request = scrapy.Request(team_url , callback=self.parse_team)
                    request.meta['division'] = division
                    yield request


    def parse_team(self, response):
        print("parsing team")
        division = response.meta.get('division')

        team_info = response.xpath("//*[@id = 'clubdetailsdiv']//tr")
        league = ""
        for row in team_info:
            if(row.xpath("th//text()")[0].extract() == "League"):
                league = row.xpath("td//a//text()")[0].extract()
        players_table = response.xpath("//*[@class = 'sortable-onload-4-6r rowstyle-alt no-arrow tabledata']//tr")
        counter = 0
        for player in players_table:

            #skip the first line
            if counter == 0:
                counter += 1
                continue

            player_url = self.BASE_URL+player.xpath('td//a//@href')[1].extract()
            print(player_url)
            if player_url is not None:
                    request = scrapy.Request(player_url, callback=self.parse_player)
                    request.meta['league'] = league
                    request.meta['division'] = division
                    yield request



    def parse_player(self, response):
        print("parsing player")

        basic_info = {}
        #get arguments
        league = response.meta.get('league')
        division = response.meta.get('division')
        basic_info['League'] = league
        basic_info['Division'] = division

        #real life table of the player
        real_life_table = response.xpath('//*[@id="realLifeTable"]//tr')

        #save this table in a dictionary in order to make the search simpler
        for row in real_life_table:
            if row.xpath('th//text()').extract()[0] not in ["Full Name", "Club", "Age", "Nation", "Date of Birth", "Height (cm)", "Weight (kg)", "Hairstyle", "Squad Number"]:
                continue

            #skip the icon for Nation field
            if(row.xpath('th//text()').extract()[0] == "Nation"):
                basic_info[row.xpath('th//text()').extract()[0]] = row.xpath('td//text()').extract()[1].strip()
            elif(row.xpath('th//text()').extract()[0] == "Full Name"):
                #print(row.xpath('td//text()').extract()[0].strip().encode('utf-8'))
                basic_info[row.xpath('th//text()').extract()[0]] = row.xpath('td//text()').extract()[0].strip()
            else:
                basic_info[row.xpath('th//text()').extract()[0]] = row.xpath('td//text()').extract()[0].strip()

        #rating
        basic_info['Rating'] = response.xpath('//*[@id="smratingdiv"]//b//text()').extract()[0]

        #attributes_table
        attributes_table = response.xpath('//*[@id="playerattributesdiv"]//span')
        attribute_list = []
        count = 0

        for row in attributes_table:
            #attribute_list.append(row.xpath('text()').extract()[0])
            basic_info["Attribute"+str(count)] = row.xpath('text()').extract()[0]
            count += 1
        #basic_info["Attributes"] = attribute_list

        while(count < 10):
            basic_info["Attribute"+str(count)] = ""
            count += 1


        #position
        position_table = response.xpath('//*[@id="playerPositionTable"]//tr')
        Positions = position_table[0].xpath('td//@title').extract()[0].split(",")
        Preferred_Foot = position_table[2].xpath('td//text()').extract()[0]
        #strip the strings of the list
        #Positions = [item.strip() for item in Positions]
        if(len(Positions) > 0):
            Positions = Positions[0].strip()
        else:
            Positions = ""
        basic_info["Positions"] = Positions
        basic_info["Preferred Foot"] = Preferred_Foot

        player = PlayerItem()
        #save this info to a player item
        for key in basic_info:
            player[key.replace(" ","_").replace("(","").replace(")","")] =  basic_info[key]

        yield player

# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PlayerItem(scrapy.Item):
    # define the fields for your item here like:
    Full_Name = scrapy.Field()
    Club = scrapy.Field()
    Age = scrapy.Field()
    Nation = scrapy.Field()
    Date_of_Birth = scrapy.Field()
    Height_cm = scrapy.Field()
    Weight_kg = scrapy.Field()
    Hairstyle = scrapy.Field()
    Squad_Number = scrapy.Field()
    Rating = scrapy.Field()
    Attributes = scrapy.Field()
    Positions = scrapy.Field()
    Preferred_Foot = scrapy.Field()
    League = scrapy.Field()
    Division = scrapy.Field()
    #TO BE TESTES
    On_Loan_at = scrapy.Field()

    pass

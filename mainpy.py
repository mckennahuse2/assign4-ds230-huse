## McKenna Huse
## Assignment 4 - SQL/Course Catalog
## DS230 FA21


import pandas as pd
import os
import requests
import json
import regex
import csv



# task 1: DB creation of student schedules

file = 'F21CourseSchedule.csv' #keep in same folder

def openFile(file):
    filelist = []
    with open(file) as f:
        linecsv = csv.reader(f)
        for row in linecsv:
            filelist.append(row)
    return filelist

def treatFile(filelist):
    ultlist = []
    baditems = []
    alttimes = []
    endcommas = regex.compile(',{5,7}')
    quotes = regex.compile("^\"(.*)\"")
    addltime = regex.compile("^(\"\",)")

    for line in filelist:
        #findendc = regex.findall(endcommas, line)
        if ''.join(line[-6:]) == ",,,,,," or ''.join(line[-7:]) == ",,,,,,,":
            pass
        elif line[0][0] == 'Course':
            pass
        else:
            ultlist.append(line)
    return ultlist

def readToSQL(listlines):
    for l in listlines:
        breakdown = l.split(',')

        print(breakdown)


def insertTable(table,row):
    q = "INSERT INTO " + str(table) + " (" \
        "VALUES ("


def dropTables():
    return """DROP TABLE schedule;"""


def createDB():
        pass

def createSchTable():
    q1 = """ CREATE TABLE schedule (
    "CourseID" INT PRIMARY KEY, 
    "Dept" VARCHAR(4),
    "Num" CHAR(3),
    "section" VARCHAR(6),
    "Title" VARCHAR(60), "Instructor" VARCHAR(20),
    "Credits" DECIMAL(10,2)
    );
    """

    q2 = """ CREATE TABLE meeting (
    "meetingID" INT PRIMARY KEY,
    "Days" SET("M","T","W","R","F"),"Begin Time" CHAR(8),"End Time" CHAR(8),
    "Bldg Room" VARCHAR(20),
    """

    return q

filecontents = openFile(file)
txcontent = treatFile(filecontents)
#readToSQL(txcontent)
print(txcontent)
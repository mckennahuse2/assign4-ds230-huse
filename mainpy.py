## McKenna Huse
## Assignment 4 - SQL/Course Catalog
## DS230 FA21


import pandas as pd
import os
import requests
import json
import regex

# task 1: DB creation of student schedules

file = 'F21CourseSchedule.csv' #keep in same folder

def openFile(file):
    filelist = []
    with open(file) as f:
        lines = f.readlines()
        for l in lines:
            filelist.append(l)
        print(filelist)
    return filelist

def treatFile(filelist):
    ultlist = []
    baditems = []
    for line in filelist:
        #print(line)
        doub = regex.compile('([\w\s\W]*)(?:,,)')
        x= regex.findall(doub,line)
        if x:
            print(x[0])
            if ',' in x[0]:
                x[0].replace(',', '')
                baditems.append(x[0])

        #ultlist.append(line)
    #print(ultlist)
    print(baditems)


def dropTables():
    return """DROP TABLE schedule;"""


def createDB():
        pass

def createSchTable():
    q = """ CREATE TABLE schedule (
    "Course Number/Title" VARCHAR(20), "Instructor" VARCHAR(20),
    "Days" SET("M","T","W","R","F"),"Begin Time" CHAR(8),"End Time" CHAR(8),
    "Bldg Room" VARCHAR(20),"Credits" DECIMAL(10,2)
    );
    """
    return q

filecontents = openFile(file)
treatFile(filecontents)
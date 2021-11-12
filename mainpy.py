## McKenna Huse
## Assignment 4 - SQL/Course Catalog
## DS230 FA21
import mysql.connector
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

def cleanFile(filelist):
    ultlist = []
    baditems = []
    alttimes = []
    endcommas = regex.compile(',{5,7}')
    quotes = regex.compile("^\"(.*)\"")
    addltime = regex.compile("^(\"\",)")

    blanks = []
    for line in filelist:
        if ''.join(line[-6:]) == '':
            dept = line[0]
            print('detected')
        elif 'Course' in line[0]:
            blanks = []
            print('course len:',len(line))
            if len(line) != 7:
                for i in range(len(line)):
                    if line[i] == '':
                        blanks.append(i)
            blanks.sort(reverse=True)
            print('blanks:',blanks)

        else:
            if len(blanks)>=1:
                for i in blanks:
                    line = line[:i] + line[i+1:]
            print(line)
            ultlist.append(line)
    return ultlist

def splitCourseCodesREGEX(courselist):
    coursedept = regex.compile('[A-Z][A-Z]?[A-Z]_')
    coursecode = regex.compile('[0-9]{3}?L_')
    coursesect = regex.compile('[0-9]{2}?W')
    for c in courselist:
        courseinfo = c[0]
        print(courseinfo)
        ccode = regex.match(coursecode,courseinfo)
        csect = regex.match(coursesect,courseinfo)
        cdept = regex.match(coursedept,courseinfo)
        csplit = courseinfo.split()
        title = ' '.join(csplit[3:])
        print(title)
        print('ccode: ',ccode)
        print('csect:',csect)

def splitCourseCodes(courseItem): # input - a row (note: all items extracted are in one list item originally
    courseinfo = courseItem[0]
    csplit = courseinfo.split()
    if len(csplit)>0:
        cdept = csplit[0]
        ccode = csplit[1]
        csect = csplit[2]
        title = ' '.join(csplit[3:])
        print(title)
        print('ccode: ',ccode)
        print('csect:',csect)
        return cdept, ccode,csect,title
    else:
        print('error:', courseinfo)
        return None

def readToSQL(listlines):
    for l in listlines:
        breakdown = l.split(',')
        print(breakdown)

def dropTables(db):
    drop =  """DROP TABLE schedule;
    DROP TABLE meeting;"""

    myc = db.cursor()
    try:
        myc.execute(drop, multi = True)
    except mysql.connector.Error as e:
        print(e)

def createSchTable(db):
    q1 = """CREATE TABLE schedule (
    CourseID INT PRIMARY KEY NOT NULL AUTO_INCREMENT, 
    Dept VARCHAR(4),
    Num CHAR(3),
    section VARCHAR(6),
    Title VARCHAR(60), Instructor VARCHAR(20),
    Credits DECIMAL(10,2)
    );
    
    CREATE TABLE meeting (
    meetingID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Days SET("M","T","W","R","F"),BeginTime CHAR(8),EndTime CHAR(8),
    Bldg VARCHAR(20)
    );
    """

    mycursor = db.cursor()

    try:
        results = mycursor.execute(q1, multi = True)
        for r in results:
            print(r)
            print('created table')
    except mysql.connector.Error as e:
        print('error creating')
        print(e)



def fillTable(db,row): #row is a list of all the items in a row
    cdept = row[0]
    ccode = row[1]
    csect = row[2]
    ctitle = row[3]
    prof = row[4]
    days = row[5]
    begin = row[6]
    end = row[7]
    bldg = row[8]
    credithr = row[9]
    q = 'INSERT INTO schedule (Dept,Num,Section,Title,Instructor,Credits) VALUES ("'\
        + cdept + '","' + ccode + '","' + csect +'","' + ctitle + '","' + prof + '","' + credithr + '"); '
    q1 = 'INSERT INTO meeting (Days,BeginTime,EndTime,Bldg) VALUES ("' + \
            days + '","' + begin + '","' + end + '"","' + bldg + '");'
    mycursor = db.cursor()
    try:
        results = mycursor.execute(q)
        db.commit()
        #results1 = mycursor.execute(q1)
        #db.commit()
        print('successful insert')


    except:
        print('error:', mysql.connector.Error)

def createDB():
    try:
        mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'dummy',
        password = 'ds230',
        database = 'mydb')
    except:
        print('error - est DB')
    return mydb

def loadSQL(db):
    filecontents = openFile(file)
    txcontent = cleanFile(filecontents)
    # readToSQL(txcontent)

    for t in txcontent:
        rowitems = []
        try:
            cdept, ccode, csect, ctitle = splitCourseCodes(t)
            rowitems = [cdept, ccode, csect, ctitle, *t[1:]]
            print(rowitems)
            fillTable(db, rowitems)
        except:
            print('insert failed')

mydb = createDB()
dropTables(mydb)
createSchTable(mydb)
loadSQL(mydb)

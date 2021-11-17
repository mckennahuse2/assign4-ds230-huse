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

def connectDB():
    try:
        mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'dummy',
        password = 'ds230',
        database = 'mydb')
    except:
        print('error - est DB')
    return mydb

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
                    if line[i] == '':
                        line = line[:i] + line[i+1:]
            if line[0] == '':
                line[0] = prevcourseinfo
            prevcourseinfo = line[0]

            credits = line[-1]
            if len(str(credits)) > 4:  # 4 characters including the decimal
                if len(str(credits)) > 4:
                    room = credits[:-5]
                    print(line[-2])
                line[-1] = credits[-4:]
            if str(credits) == '':
                if 'Athletics and' in str(line[-2]):
                    bldgARC = line[-2][:-5]
                    creditARC = line[-2][-4:]
                    line[-2] = bldgARC
                    line[-1] = creditARC
                #line[-1] = prevcredits

            prevcredits = line[-1]
            print(line)
            ultlist.append(line)

    return ultlist

def splitCourseCodes(courseItem): # input - a row (note: all items extracted are in one list item originally
    courseinfo = courseItem[0]
    csplit = courseinfo.split()

    if len(csplit)>0:
        if len(csplit[0]) > 3: # if the coursedept and course code are combined (no space separating )..
            ccode = csplit[0][-3:]
            cdept = csplit[0][:-3]
            csect = csplit[1]
            title = ' '.join(csplit[2:])
        else:
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
    Num VARCHAR(4),
    section VARCHAR(6),
    Title VARCHAR(80), Instructor VARCHAR(20),
    Credits VARCHAR(5)
    );
    
    CREATE TABLE meeting (
    meetingID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    CourseID INT,
    Days VARCHAR(8),BeginTime VARCHAR(8),EndTime VARCHAR(8),
    Bldg VARCHAR(30)
    );
    """

    #Days SET("M","T","W","R","F")
    # tried to set up days with set datatype - was causing issues inserting classes w/o days (arranged classes)
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

    qcID = 'SELECT CourseID from schedule where Dept = "' + str(cdept) + \
           '" AND Num  = "' + str(ccode) + '" and section = "'+ str(csect)+'";'

    q1 = 'INSERT INTO meeting (CourseID,Days,BeginTime,EndTime,Bldg) VALUES (' + \
         '(SELECT CourseID from schedule where Dept = "' + cdept + \
         '" AND Num = "' + ccode + '" AND section = "'+ csect+'"), "' + \
            days + '","' + begin + '","' + end + '","' + bldg + '");'

    print(q)
    mycursor = db.cursor()
    try:
        if credithr != '':
            results = mycursor.execute(q, multi=True)
            db.commit()
            print('successful insert')

    except:
        print('error:', mysql.connector.Error)

    try:
        print('attempt:', q1)
        res = mycursor.execute(q1)
        db.commit()
        print('successful meeting insert')
    except mysql.connector.Error as e:
        print(e)
        print('failed meeting insert')




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
            #fixCreditsVals(rowitems)
            fillTable(db, rowitems)
        except:
            print('insert failed')


def taskpt1():
    mydb = connectDB()
    dropTables(mydb)
    createSchTable(mydb)
    loadSQL(mydb)

### task 2
# simple queries w/ schedule

def executeQuery(mydb,query):
    mycursor = mydb.cursor()
    try:
        mycursor.execute(query)
        result = mycursor.fetchall() # thanks w3schools (https://www.w3schools.com/python/python_mysql_select.asp)

    except mysql.connector.Error as e:
        print(e)
        print('error: is there an error in your syntax?')
        result = None
    return result

def courseDept(mydb,dept): # enter department as 3 letter code
    q = 'SELECT * FROM schedule WHERE Dept = "' + dept + '";'
    results = executeQuery(mydb,q)
    print(results)
    return results

def timeBlock():
    pass

def socialSciClasses(mydb,**chosendepts): # optional argument to override course dept list
    ssDepts = ['ANT',"ECO","POL","PSY","SOC"]
    if chosendepts:
        ssDepts = chosendepts
    q = 'SELECT * from schedule where Dept = "'
    for s in ssDepts[:-1]:
         q+= s + '" OR Dept = "'
    q+= ssDepts[-1] + '";'
    results = executeQuery(mydb,q)
    print(results)
    return results

def dcpCourses(mydb):
    q = 'SELECT * from schedule WHERE RIGHT(Num,1) IN ("6","7","8");'
    results = executeQuery(mydb,q)
    print(results)
    return results

def courseByProf(mydb,prof): # prof in FI Lastname format
    q = 'SELECT * from schedule WHERE Instructor LIKE "' + prof + '";'
    results = executeQuery(mydb, q)
    print(results)
    return results

## task 3 : course registration
def createStudentEnrollTable(mydb):
    drop = """ DROP TABLE student; DROP TABLE enrollment;"""

    q = '''CREATE TABLE student (
    studentID INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    studentNum VARCHAR(10),
    fname VARCHAR(20),
    lname VARCHAR(20),
    classYear VARCHAR(4),
    major1 VARCHAR(20),
    major2 VARCHAR(20),
    minor1 VARCHAR(20),
    advisor VARCHAR(20)
    );
    
    CREATE TABLE enrollment (
    studentID INT NOT NULL,
    courseID INT NOT NULL,
    status ENUM("Active","Waitlist"),
    FOREIGN KEY (studentID) references student(studentID), FOREIGN KEY (courseID) references schedule(CourseID));
    '''

    mycd = mydb.cursor()
    try:
        mycd.execute(drop, multi = True)
        print('prior tables dropped')
    except mysql.connector.Error as e:
        print(e)

    myc = mydb.cursor()
    try:
        myc.execute(q, multi= True)
        print('success')
    except mysql.connector.Error as e:
        print(e)

def fillRegistration(mydb):



mydb = connectDB()
dropTables(mydb)
createSchTable(mydb)
loadSQL(mydb)
createStudentEnrollTable(mydb)
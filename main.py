import uvicorn
from models import Sample
from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
import sqlite3, argparse

db = sqlite3.connect("example.db")
cur = db.cursor()

app = FastAPI()

#START OF FUNCTIONS
def initializeDB():
        #initializes DB, drops table if it exists prior to initialization
        #does not drop tables uniform/normal/weibull, as entries from those
        #are automatically deleted when the foreign key is deleted via dropping samples
        cur.execute('''DROP TABLE IF EXISTS `samples`''')
        cur.execute(
            '''CREATE TABLE `samples`(
                `ID` INTEGER PRIMARY KEY,
                `DistributionType` TEXT,
                `DataPoints` TEXT,
                `ValCount` INTEGER,
                CHECK (`DistributionType` in ('uniform','normal','weibull'))
            )''')
        cur.execute(
            '''CREATE TABLE IF NOT EXISTS`uniform`(
                `ID` INTEGER,
                `low` REAL,
                `high` REAL,
                FOREIGN KEY(ID) REFERENCES `samples`(ID))
            ''')
        cur.execute(
            '''CREATE TABLE IF NOT EXISTS `normal`(
                `ID` INTEGER,
                `loc` REAL,
                `scale` REAL,
                FOREIGN KEY(ID) REFERENCES `samples`(ID))
            ''')
        cur.execute(
            '''CREATE TABLE IF NOT EXISTS `weibull`(
                `ID` INTEGER,
                `shape` REAL,
                FOREIGN KEY(ID) REFERENCES `samples`(ID))
            ''')
        #commits the current state of the db, as it is now no longer being updated
        db.commit()

#this function checks if the ID exists in the database, and raises an http error if it does not
def checkIfIDExists(i:int):
    try:
        sqlQuery = f"SELECT * FROM `samples` AS s WHERE s.ID = {i}"
        r = cur.execute(sqlQuery).fetchall()
    except:
        raise HTTPException(status_code=400, detail=f"The ID: {i} has caused an error, please ensure that it is a number and try again")
    return r

#processes SQL responses into suitable dictionary for further use
def processSQLResult(SQLResponse):
    p = dict()
    for row in SQLResponse:
            id = row[0]
            p[id] = dict()
            p[id]["id"] = id
            p[id]["distributionType"] = row[1]
            #split string into list of strings, then convert to a list of integers
            p[id]["values"] = list(map(int,row[2].split(",")))
            p[id]["sampleCount"] = row[3]
    return p

#distribution specific methods to insert the additional required parameters into suitable tables
#TODO SQLQuerys for inserting appropriate additonal parameters into distributiontype tables
def insertWeibull(sample):
    #db.commit()
    pass

def insertUniform(sample):
    #db.commit()
    pass

def insertNormal(sample):
    #db.commit()
    pass

#END OF FUNCTIONS

#START OF REST
@app.get('/')
async def root():
    return {"Hello": "Please add /docs to the address to interact with the API"}

#overwrites default response to erroneous requests
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return PlainTextResponse('''    There was an error while validating a request, most likely this was caused by a bad request while attempting to add a new sample.
    please ensure these request fields are correct, Example:
        {
            "distributionType": "uniform" OR "normal" OR "weibull",
            "values":[1,2,3]
        }
    Additionally, ensure that optional fields such as id (int) and sampleCount (int) are either not present, or are properly input'''
    , status_code=400)

@app.get("/api/samples")
async def get_samples():
    #check if db is somehow not connected
    if(db == None):
        raise HTTPException(status_code=404, detail="the db has not been connected")
    else:
        #select all rows from sample database, process data into a dictionary and return the processed dictionary
        SQLResponse = cur.execute("SELECT * FROM `samples`").fetchall()
        if(SQLResponse == []):
            raise HTTPException(status_code=404, detail="the db currently has no data")
        processedList = processSQLResult(SQLResponse)
        return processedList

@app.post("/api/sample")
async def add_sample(sample: Sample):
    if(db == None):
        raise HTTPException(status_code=404, detail="the db has not been connected")
    else:
        #check if samplecount is set and correct, otherwise set it as amount of values
        if(sample.sampleCount != len(sample.values)):
            sample.sampleCount = len(sample.values)
        if(sample.sampleCount <= 0):
            raise HTTPException(status_code=400, detail=f"The sample {sample} has no values in the value list")

        #check if request has a sample ID defined, if not, do query that uses database generated id
        if(sample.id != None):
            #check if ID exists in database if defined in request
            if(processSQLResult(checkIfIDExists(sample.id)) != {}):
                raise HTTPException(status_code=400, detail=f"the sample with id: {sample.id} already exists in the database")
            #Format query and execute, ensure cleaning the request values list of brackets to remove potential problems later
            SQLQuery = "INSERT INTO `samples` (`ID`,`DistributionType`,`DataPoints`, `ValCount`) VALUES ({id},'{distr}','{set}',{size})".format(id = sample.id, distr=sample.distributionType, set=str(sample.values).strip("[]"), size = sample.sampleCount)
            cur.execute(SQLQuery)
        else:
            SQLQuery = "INSERT INTO `samples` (`DistributionType`,`DataPoints`, `ValCount`) VALUES ('{distr}','{set}',{size})".format(distr=sample.distributionType, set=str(sample.values).strip("[]"), size = sample.sampleCount)
            cur.execute(SQLQuery)
            sample.id = cur.lastrowid

        #call a suitable follow-up insert function based on distribution type
        if(sample.distributionType == "weibull"):
            insertWeibull(sample)
        if(sample.distributionType == "uniform"):
            insertUniform(sample)
        if(sample.distributionType == "normal"):
            insertNormal(sample)
        #commit changes to DB as it has now finished inserting
        db.commit()
        return f"The request to add the distribution has been completed, and may be found with ID: {sample.id}"

@app.get("/api/sample/{sample_id}/statistics")
async def get_sample(sample_id):
    if(db == None):
        raise HTTPException(status_code=404, detail="the db has not been connected")
    else:
        #check if sample exists and raise error if it doesn't.
        #process the sample to be able to perform calculations
        result = processSQLResult(checkIfIDExists(sample_id))
        if(result == {}):
            raise HTTPException(status_code=404, detail=f"The ID {sample_id} does not exist in the database")
        return result

        #TODO do distribution calcs

#END OF REST

#start of program, starts uvicorn server
def main(flag):
    if(flag):
        initializeDB()
    uvicorn.run("main:app", reload = True)

if __name__ == '__main__':
    f = False

    #parse commandline arguments
    parser = argparse.ArgumentParser()
    #TODO rework entire database implementation to allow for specifying database
    #parser.add_argument("-d", "--database", default="example.db", help="Specifies the database to be used")
    parser.add_argument("--reset", help = "!!type 'py .\main.py --reset true' to reset the entire database to initial values!!")

    args = parser.parse_args()
    if(args.reset == "true"):
        f = True
    main(f)

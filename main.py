import datetime
import shutil
import subprocess
import os
import re
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from enum import Enum
from typing import Optional, Tuple, List
from components import get_weather


#
# Define Env const
# Todo: to env variable
WTH_PATH = os.getcwd()
SOL_PATH = 'CSM'
X_PATH = 'CSM'

# GITHUB TEST
#
# Run, Parse CSM
#
def run_csm(xfile):
    #
    # Run CSM
    #
    print(xfile)
    subprocess.call('docker run --rm -v {0}/temp:/data -w /data dssat-csm A {1}'.format(
        os.getcwd(),
        xfile
    ), shell=True)

    #
    # Parse (TODO)
    #
    def parse(output_dir):
        # Summary
        def parse_summary(summary_out):
            with open(os.path.join(output_dir, summary_out), 'r') as so:
                with open('temp/Summary.OUT', 'r') as sm:
                    content = sm.readlines()
                sm_val = [x for x in content[-1].split(' ') if x]
                sm_key = [x.strip('.') for x in content[-2].split(' ') if x][1:]
            return {key: val for key, val in zip(sm_key, sm_val)}

        # MgmtEvent
        def parse_mgmtevent(mgmtevent):
            with open(os.path.join(output_dir, mgmtevent), 'r') as me:
                keys = ['RUN',
                        'Date',
                        'DOY',
                        'DAS',
                        'DAP',
                        'CR',
                        'Stage',
                        'Operation',
                        'Quantities']
                events = []
                for i, line in enumerate(me):
                    if i >= 6:
                        event = [x.strip('.').strip() for x in line.split('  ') if x.strip()]
                        if len(event) == 0:
                            break
                        # Formatting
                        run_no = event[0][0]
                        event[0] = event[0][1:]
                        if len(event[0]) <= 4:
                            month = ''.join([x for x in event.pop(0) if not x.isdigit()])
                            event[0] = month + ' ' + event[0]
                        event = [run_no] + event
                        event = [x.strip() for x in event]

                        # Make to key-val
                        if len(event) < len(keys):
                            event.insert(6, 'NaN')
                            if len(event) < len(keys):
                                event.insert(8, 'NaN')
                        event = {key: val for key, val in zip(keys, event)}
                        events.append(event)
            return events

        result = {'summary': parse_summary('Summary.OUT'),
                  'events': parse_mgmtevent('MgmtEvent.OUT')}

        return result

    return parse('temp')


#
# Build API Server
#
app = FastAPI()


class CropCode(str, Enum):
    BA = 'Barley',
    RI = 'Rice'


class FarmInfo(BaseModel):
    xcoord: float
    ycoord: float
    day: Optional[datetime.datetime] = None
    mgmthistory: Optional[list] = None


class EventOut(BaseModel):
    RUN: int
    Date: str
    DOY: int
    DAS: int
    DAP: int
    CR: str
    Stage: str
    Operation: str
    Quantities: str


class SummaryOut(BaseModel):
    RUNNO: int
    CR: str
    MODEL: str
    EXNAME: str
    OPTAM: int


class ResponseOut(BaseModel):
    summary: SummaryOut
    events: List[EventOut]


@app.get("/")
async def root():
    return {"message": "CSM Server"}


@app.get("/{crop}", response_model=ResponseOut)
async def main(crop: CropCode, farm: FarmInfo):
    #
    # Check Farmland by Coordinate
    #
    # farmland = FarmLand.SAMPLE

    #
    # Make Temp Dir.
    #
    if os.path.exists('temp'):
        shutil.rmtree('temp')
    os.mkdir('temp')

    #
    # Create Farmland's Weather DATA
    #
    whether_code = 'SFKR'
    get_weather.get_weather(farm.xcoord, farm.ycoord, code=whether_code)
    whether_files = [x for x in os.listdir(os.getcwd()) if x.startswith(whether_code)]
    for wth in whether_files:
        shutil.copy(os.path.join(WTH_PATH, wth), os.path.join('temp', wth))

    #
    # Copy Farmland's SOL file to pwd
    #
    soil_code = 'CO'
    soil_files = [x for x in os.listdir(SOL_PATH) if x.startswith(soil_code)]
    for sol in soil_files:
        shutil.copy(os.path.join(SOL_PATH, sol), os.path.join('temp', sol))

    #
    # Copy Crop's Sample X File to pwd
    #
    crop_code: str = crop.name
    x_file = [x for x in os.listdir(X_PATH) if x.endswith(crop_code.upper()+'X')][0]

    shutil.copy(os.path.join(X_PATH, x_file), os.path.join('temp', x_file))

    #
    # Modify with Requested Data
    #

    #
    # Run and Parse CSM
    #
    print(x_file)
    output = run_csm(x_file)
    shutil.rmtree('temp')

    return JSONResponse(content=jsonable_encoder(output))

    #
    # Send Response
    #

    #
    # Delete Temp Files
    #




import numpy as np
import pandas as pd
import pywt as wt

from scipy.interpolate import interp1d

import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import Row

from hdfs import *

ALL_GroupFiles = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 111, 112, 113, 114, 115,
                  116, 117, 118, 119, 121, 122, 123, 124, 200, 201, 202, 203, 205,
                  207, 208, 209, 210, 212, 213, 214, 215, 217, 219, 220,
                  221, 222, 223, 228, 230, 231, 232, 233, 234]
MLII_1_GroupFiles = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 111, 112, 113, 115,
                     116, 117, 118, 119, 121, 122, 123, 124, 200, 201, 202, 203, 205,
                     207, 208, 209, 210, 212, 213, 214, 215, 217, 219, 220,
                     221, 222, 223, 228, 230, 231, 232, 233, 234]
MLII_2_GroupFiles = [114]
V1_2_GroupFiles = [101, 105, 106, 107, 108, 109, 111, 112, 113, 115,
                   116, 118, 119, 121, 122, 200, 201, 202, 203, 205,
                   207, 208, 209, 210, 212, 213, 214, 215, 217, 219, 220,
                   221, 222, 223, 228, 230, 231, 232, 233, 234]
V2_2_GroupFiles = [102, 103, 104, 117]
V4_2_GroupFiles = [124]
V5_1_GroupFiles = [102, 104, 114]
V5_2_GroupFiles = [100, 123]


def Interpolation_RR_Interval(i):
    MaxLength = broadCast_MaxLength.value
    return MaxLength


def Interpolation_TimeIndex(i):
    MaxLength = broadCast_MaxLength.value

    # if i == 0:
    #    IntropR_TimeIndex = broadCast_R_TimeIndex.value[0];

    # if i != len(broadCast_RR_interval.value):
    #    IntropR_TimeIndex = broadCast_R_TimeIndex.value[0] + MaxLength*i
    Temp_IntropR_TimeIndex = broadCast_R_TimeIndex.value[0] + MaxLength * i

    return Temp_IntropR_TimeIndex


def Interpolation_Signal(i):
    MaxLength = broadCast_MaxLength.value
    # Temp_Time           = Orig_Time(R_TimeIndex(i):R_TimeIndex(i)+RR_interval(i)-1)
    # Temp_Introp_Time    = Temp_Time
    indexBegin = broadCast_R_TimeIndex.value[i]
    indexEnd = broadCast_R_TimeIndex.value[i] + broadCast_RR_interval.value[i] - 1

    # Temp_Signal         = Orig_Signal(R_TimeIndex(i):R_TimeIndex(i)+RR_interval(i)-1)
    # Temp_Introp_Signal  = Temp_Signal.';
    Temp_Signal = broadCast_Orig_Signal.value[indexBegin: indexEnd]
    Temp_Introp_Signal = Temp_Signal

    if broadCast_RR_interval.value[i] < MaxLength:  # length of RR is less than largest RR interval

        # m = 1
        # k = 1
        m = 0
        k = 0

        while len(Temp_Introp_Signal) < MaxLength:
            if m >= len(Temp_Signal):
                # m = 1
                # k = 1
                m = 0
                k = 0

                Temp_Signal = Temp_Introp_Signal

            Signal_mean = (Temp_Signal[m] + Temp_Signal[m + 1]) / 2

            # Temp_Introp_Signal = [Temp_Introp_Signal(1:k),Signal_mean,Temp_Introp_Signal(k+1:end)];
            Temp_Introp_Signal.insert(k + 1, Signal_mean)

            k = k + 2
            m = m + 1
            # change_length=change_length+1;

    #Introp_Signal = Temp_Introp_Signal

    return Temp_Introp_Signal


def Interpolation_Time(i):
    MaxLength = broadCast_MaxLength.value

    indexBegin = broadCast_R_TimeIndex.value[i]
    indexEnd = broadCast_R_TimeIndex.value[i] + broadCast_RR_interval.value[i] - 1

    Temp_Time = broadCast_Orig_Time.value[indexBegin: indexEnd]
    Temp_Introp_Time = Temp_Time

    if broadCast_RR_interval.value[i] < MaxLength:  # length of RR is less than largest RR interval

        # m = 1
        # k = 1
        m = 0
        k = 0

        while len(Temp_Introp_Time) < MaxLength:
            if m >= len(Temp_Time):
                # m = 1
                # k = 1
                m = 0
                k = 0

                Temp_Time = Temp_Introp_Time

            Time_mean = (Temp_Time[m] + Temp_Time[m + 1]) / 2

            # Temp_Introp_Time = [Temp_Introp_Time(1:k),Time_mean,Temp_Introp_Time(k+1:end)];
            Temp_Introp_Time.insert(k + 1, Time_mean)

            k = k + 2
            m = m + 1
            # change_length=change_length+1;

    #Introp_Time = Temp_Introp_Time

    return Temp_Introp_Time


def floatrange(start, stop, steps):
    ''' Computes a range of floating value.

        Input:
            start (float)  : Start value.
            end   (float)  : End value
            steps (integer): Number of values

        Output:
            A list of floats

        Example:
            >>> print floatrange(0.25, 1.3, 5)
            [0.25, 0.51249999999999996, 0.77500000000000002, 1.0375000000000001, 1.3]
    '''
    return [start + float(i) * (stop - start) / (float(steps) - 1) for i in range(steps)]

def Interpolation():


    HDFSPath = '/Interpolation'
    HDFSURL = 'http://Master.Hadoop:50070'

    client = Client(HDFSURL, root= '/')
    client.delete(HDFSPath, True)



    startTimeMain = datetime.datetime.now()

    RRInfoFilePath = '/home/terence01/workspace/Spark/Interpolation/RRInfo/'
    txtFilePath = '/home/terence01/workspace/Spark/Interpolation/TextData/'

    OutPutPath = 'hdfs://Master.Hadoop:9000/Interpolation/'

    icounter = 0

    for icounter in range(len(ALL_GroupFiles)):

        FileName = str(ALL_GroupFiles[icounter])
        BeatTypeFile = RRInfoFilePath + FileName + "_BeatType.txt"
        VoltageFile = txtFilePath + FileName + ".txt"
        startTimeEachFile = datetime.datetime.now()

        print "<----> Interpolation Main Processing %s begin\n" % (VoltageFile)

        print "<----> Interpolation Main load dataframe for beatType\n"
        dataFrame = pd.read_csv(BeatTypeFile, delim_whitespace=True)

        print "<----> Interpolation Main load dataframe for M\n"
        M = pd.read_csv(VoltageFile, delim_whitespace=True)

        #    #Remove the first RR interval and 1 unit shift forward
        #    R_TimeIndex = R_TimeIndex(2:end)+1;
        #    RR_interval = RR_interval(2:end)+1;

        print "<----> Interpolation Main Remove the first RR interval and 1 unit shift forward\n"
        dataFrame['R_TimeIndex'] = dataFrame['R_TimeIndex'] + 1
        R_TimeIndex = dataFrame.loc[:, 'R_TimeIndex'].tolist()
        del (R_TimeIndex[0])

        ## need to transfer to spark
        dataFrame['RR_interval'] = dataFrame['RR_interval'] + 1
        RR_interval = dataFrame.loc[:, 'RR_interval'].tolist()
        del (RR_interval[0])

        print "<----> Interpolation Main MaxLength = 2114\n"
        MaxLength = 2114

        #    #remove the first one and the last one
        #    ann = ann(2:end-1);
        #    type = type(2:end-1);
        endIndex = dataFrame['BeatDuration'].count() - 1;
        ann = dataFrame.loc[1:, 'BeatDuration'].tolist()
        type = dataFrame.loc[1:, 'BeatType'].tolist()

        for iNumberLead in range(1, 3):
            columnName = 'Voltage_Lead%d' % (iNumberLead)
            print "<----> Interpolation Main Processing %s.txt %s begin\n" % (FileName, columnName)
            startTimeProcessing = datetime.datetime.now()
            startTimeDenosing = datetime.datetime.now()
            print "<----> Interpolation Main Denosing for %s.txt %s begin\n" % (FileName, columnName)

            RawSignal = M.loc[:, columnName].tolist()
            Length_RawSignal = len(RawSignal)
            # Denosing
            sym5 = wt.Wavelet('sym5')
            coeffs = wt.wavedec(RawSignal, sym5, 'sym', level=8)

            cA8, cD8, cD7, cD6, cD5, cD4, cD3, cD2, cD1 = coeffs
            cNA8 = np.zeros(np.size(cA8))
            cND1 = np.zeros(np.size(cD1))
            cND2 = np.zeros(np.size(cD2))
            cND3 = np.zeros(np.size(cD3))
            cND4 = np.zeros(np.size(cD4))
            cND5 = np.zeros(np.size(cD5))
            cND6 = np.zeros(np.size(cD6))
            cND7 = np.zeros(np.size(cD7))
            cND8 = np.zeros(np.size(cD8))

            Denosied_Signal = wt.waverec((cNA8, cND8, cD7, cD6, cD5, cD4, cD3, cD2, cD1), 'sym5')
            endTimeDenosing = datetime.datetime.now()
            print "<----> Interpolation Main Denosing for %s end, execute time %ds\n" % (columnName, (endTimeDenosing - startTimeDenosing).seconds)
            # End Denosing

            # Interpolation
            startTimeInterpolation = datetime.datetime.now()
            print "<----> Interpolation Main Interpolation for %s begin\n" % (columnName)
            # (R_TimeIndex(1):R_TimeIndex(end)+RR_interval(end),1);
            # Orig_Signal         = Denosied_Signal
            Orig_Signal = Denosied_Signal.tolist()
            # (R_TimeIndex(1):R_TimeIndex(end)+RR_interval(end));
            # Orig_Time           = TIME
            Orig_Time = M.loc[:, 'Time'].tolist()

            # IntropR_TimeIndex   = R_TimeIndex(1)
            IntropR_TimeIndex = []
            IntropR_TimeIndex.append(R_TimeIndex[0])

            # IntropRR_interval   = RR_interval(1)
            IntropRR_interval = []
            IntropRR_interval.append(RR_interval[0])

            RR_interval_count = len(RR_interval)

            print "<----> Interpolation Main parallelize Interpolation_RR_Interval\n"
            rdd_IntropRR_interval = sc.parallelize(range(RR_interval_count)).map(lambda i: MaxLength)
            IntropRR_interval = rdd_IntropRR_interval.collect()
            rdd_IntropRR_interval.unpersist()


            print "<----> Interpolation Main parallelize Interpolation_TimeIndex\n"
            rdd_IntropR_TimeIndex = sc.parallelize(range(RR_interval_count)).map(lambda i: R_TimeIndex[0] + MaxLength * i)
            IntropR_TimeIndex = rdd_IntropR_TimeIndex.collect()
            rdd_IntropR_TimeIndex.unpersist()

            f1 = interp1d(Orig_Time, Orig_Signal)

            startTime = Orig_Time[0]
            endTime = Orig_Time[len(Orig_Time) - 1]
            step = (endTime - startTime) / (MaxLength * RR_interval_count)

            print "<----> Interpolation main parallelize Interpolation_Time\n"
            rdd_Introp_Time = sc.parallelize(range(MaxLength * RR_interval_count)).map(lambda i: Row(i, startTime + float(i) * step))

            Introp_Time = rdd_Introp_Time.map(lambda row: row[1]).collect()

            rdd_Introp_Time.unpersist()

            print "<----> Interpolation Main parallelize Interpolation_Signal\n"
            Introp_Signal = list(f1(Introp_Time))


            endTimeInterpolation = datetime.datetime.now()
            print "<----> Interpolation Main Interpolation for %s end, execute time %d s\n" % (columnName, (endTimeInterpolation - startTimeInterpolation).seconds)

            ## Interpolation end

            # Check BeatType
            print "<----> Interpolation Main Check BeatType begin\n"
            startTimeBeatType = datetime.datetime.now()
            BeatType = ['N', 'L', 'R', 'B', 'A', 'a', 'J', 'S', 'V', 'r', 'F', 'e', 'j', 'n', 'E', '/', 'f', 'Q',
                        '?']  # remove'|'
            # BeatIndex       = 1
            BeatIndex = 0
            # BeatTypeArray   = type(1)
            BeatTypeArray = []  # type(1)
            # BeatTimeArray   = ann[1]
            BeatTimeArray = []  # ann(1);

            for i in range(len(type)):
                # for j in range(len(BeatType)):
                if type[i] in BeatType:
                    BeatTypeArray.append(type[i])
                    BeatTimeArray.append(ann[i])
            endTimeBeatType = datetime.datetime.now()
            print "<----> Interpolation Main Check BeatType end, execute time %ds\n" % (
                (endTimeBeatType - startTimeBeatType).seconds)
            # Check BeatType end

            # Log to file
            LogToFile = True

            if LogToFile == True:

                leadstr = '_NON_'
                if iNumberLead == 1:
                    if ALL_GroupFiles[icounter] in V5_1_GroupFiles:
                        leadstr = '_V5_'

                    if ALL_GroupFiles[icounter] in MLII_1_GroupFiles:
                        leadstr = '_MLII_'

                if iNumberLead == 2:
                    if ALL_GroupFiles[icounter] in V5_2_GroupFiles:
                        leadstr = '_V5_'

                    if ALL_GroupFiles[icounter] in V1_2_GroupFiles:
                        leadstr = '_V1_'

                    if ALL_GroupFiles[icounter] in V2_2_GroupFiles:
                        leadstr = '_V2_'

                    if ALL_GroupFiles[icounter] in V4_2_GroupFiles:
                        leadstr = '_V4_'

                    if ALL_GroupFiles[icounter] in MLII_2_GroupFiles:
                        leadstr = '_MLII_'


                OutPutFileName = '/Interpolation/' + str(ALL_GroupFiles[icounter]) + leadstr + str(MaxLength) + '.csv'
                OutPutFileName2 = '/Interpolation/' + str(ALL_GroupFiles[icounter]) + leadstr + '_RR_' + str(MaxLength) + '.csv'

                LogFileOption1 = True

                startTimeLogFile = datetime.datetime.now()

                if LogFileOption1 == True:
                    print "<----> Interpolation Main Log file %s\n" % (OutPutFileName)
                    print "<----> Interpolation Main Log file %s, step 1\n" % (OutPutFileName)
                    temp_df = pd.DataFrame({"Time": Introp_Time, "Voltage": Introp_Signal})

                    print "<----> Interpolation Main Log file %s, step 2\n" % (OutPutFileName)
                    csv_content = temp_df.to_csv(index = False)

                    print "<----> Interpolation Main Log file %s, step 3\n" % (OutPutFileName)
                    try:
                        client.write(OutPutFileName, csv_content, overwrite=True)
                    finally:
                        print "<----> Interpolation Main Log file %s, step 4\n" % (OutPutFileName)
                        del temp_df

                    print "<----> Interpolation Main Log file %s\n" % (OutPutFileName2)
                    TimeIndex_Len = len(IntropR_TimeIndex)
                    IntropRR_interval_Len = len(IntropRR_interval)
                    BeatType_Len = len(BeatTypeArray)

                    if ((TimeIndex_Len != IntropRR_interval_Len) or (TimeIndex_Len != BeatType_Len) or (
                                IntropRR_interval_Len != BeatType_Len)):
                        print "<---->Error Interpolation Main Log file %s\n" % (OutPutFileName2)
                        continue

                    print "<----> Interpolation Main Log file %s, step 1\n" % (OutPutFileName2)
                    temp_df2 = pd.DataFrame({"Start_Time": IntropR_TimeIndex, "RR_Interval": IntropRR_interval, "BeatType": BeatTypeArray})

                    print "<----> Interpolation Main Log file %s, step 2\n" % (OutPutFileName2)
                    csv_content2 = temp_df2.to_csv(index = False)

                    print "<----> Interpolation Main Log file %s, step 3\n" % (OutPutFileName2)
                    try:
                        client.write(OutPutFileName2, csv_content2, overwrite=True)
                    finally:
                        print "<----> Interpolation Main Log file %s, step 4\n" % (OutPutFileName2)
                        del temp_df2

                endTimeLogFile = datetime.datetime.now()
                print "<----> Interpolation Main Log file for %s end, execute time %ds\n" % (columnName, (endTimeLogFile - startTimeLogFile).seconds)

            endTimeProcessing = datetime.datetime.now()
            print "<----> Interpolation Main Processing %s end, execute time %ds\n" % (columnName, (endTimeProcessing - startTimeProcessing).seconds)
            # Log to file end

        endTimeEachFile = datetime.datetime.now()
        print "<----> Interpolation Main Processing %s end, execute time %ds\n" % (VoltageFile, (endTimeEachFile - startTimeEachFile).seconds)

    endTimeMain = datetime.datetime.now()
    print "<----> Interpolation Main end, execute time %ds\n" % ((endTimeMain - startTimeMain).seconds)

# End of 'Interpolation'

if __name__ == "__main__":
    conf = SparkConf().setAppName('Interpolation')
    conf.set("spark.executor.heartbeatInterval", "60s")
    sc = SparkContext(conf = conf)

    Interpolation()
# End of 'main'

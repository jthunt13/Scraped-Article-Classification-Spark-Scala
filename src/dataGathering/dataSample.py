import pandas as pd
import numpy as np
import os
import time
import random
import math
from shutil import copyfile

def countCheck(topic,article_count):
    fnames = os.listdir("../../data/raw/" + topic + "/")
    if len(fnames) == article_count:
        return(True)
    else:
        return(False)

def diff(first, second):
        second = set(second)
        return [item for item in first if item not in second]

def sanityCheck(data):
    sanity = True
    for df in data:
        dst = "../../data/" + df.name + "/" + topic + "/"

        if df.name == 'train':
            train_fnames = os.listdir(dst)
            if len(train_fnames) != train_length:
                print(topic + ': ' + '# of train files(' + str(len(train_fnames)) + ')' +
                    ' != expected length(' + str(train_length) +')' )
                sanity = False

        elif df.name == 'test':
            test_fnames = os.listdir(dst)
            if len(test_fnames) != test_length:
                print(topic + ': ' + '# of test files(' + str(len(test_fnames)) + ')' +
                    ' != expected length(' + str(test_length) +')' )
                sanity = False

        elif df.name == 'validate':
            validate_fnames = os.listdir(dst)
            if len(validate_fnames) != validate_length:
                print(topic + ': ' + '# of validate files(' + str(len(validate_fnames)) + ')' +
                    ' != expected length(' + str(validate_length) +')' )
                sanity = False

    fnames = train_fnames + test_fnames + validate_fnames

    if len(fnames) != len(set(fnames)):
        dups = diff(fnames,set(fnames))
        print(topic + ': duplicate files found')
        print(dups)
        sanity = False

    if sanity == False:
        print(topic + ': Sanity check failed, see errors above')

    else:
        print(topic + ': Sanity check passed')
    print('')

def fileMover(topic,data):
    src = "../../data/raw/" + topic + "/"
    for df in data:
        dst = "../../data/" + df.name + "/" + topic + "/"
        fnames = os.listdir(dst)
        for row in df.itertuples():
            file = row[1] + '.txt'
            if file in fnames:
                continue
            else:
                doc_src = src + file
                doc_dst = dst + file
                copyfile(doc_src,doc_dst)
                fnames.append(file)
    sanityCheck(data)
#------------------------------------------------------------------------------
#                               Begin Main Script
#------------------------------------------------------------------------------
if __name__ == "__main__":
    random.seed(1)
    global article_count
    global split
    global train_length
    global test_length
    global validate_length

    topics = ['politics','sports','business','science']
    article_count = 500 # Specify the number of articles expected for each topic
    split = [60,30,10] # Specify how split the data [Train %, Test %, Validate %]

    train_length = math.floor(article_count * (split[0]/100))
    test_length = math.floor(article_count * (split[1]/100))
    validate_length = article_count - train_length - test_length

    for topic in topics:
        if countCheck(topic,article_count) == False: # check that enough articles exist
            print("Not enough articles for: " + topic)
        else:
            filename = "../../data/raw/" + topic + ".csv"
            df = pd.read_csv(filename)
            df = df.drop(df.columns[[0,2,3]], axis = 1)

            # Randomize train set
            train_ind = np.sort(random.sample(range(0, article_count),train_length))

            # Find the remaining indices and randomize test set
            rows_left = diff(list(range(0,article_count)),train_ind)
            test_ind = np.sort(random.sample(rows_left,test_length))

            # Use remaining rows for validation set
            remaining_rows = diff(rows_left,test_ind)

            train = df.iloc[train_ind,]
            train.name = 'train'

            test = df.iloc[test_ind,]
            test.name = 'test'

            validate = df.iloc[remaining_rows,]
            validate.name = 'validate'

            data = [train,test,validate]
            fileMover(topic,data)

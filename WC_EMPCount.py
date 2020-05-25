'''
Created on Aug 29, 2019

@author: michael
'''
import os
import dbf
import csv
from dbf import READ_ONLY

def main():
    #Declaring main variables CSR/Drive
    wc_csv = input("Please CSV NAME: ")
    EmpNumList = []
    co_csv(wc_csv, EmpNumList)
    pol(EmpNumList)
    
##reading csv of Company Codes and Names
def co_csv(wc_csv,EmpNumList):
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    with open(wc_csv) as csv_file: 
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            rec1 = row[0]
            rec2 = rec1[0:7]
            rec3 = rec1[7:9]
            rec4 = rec2 + 'C' + rec3
            EmpNumList.append(rec4)
            line_count += 1
        print (line_count)

def pol(EmpNumList):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")

    table = dbf.Table("AP1DBDB.DBF")
    table.open(mode=READ_ONLY)
    table.use_deleted = False
    
    ##Declaring Variables
    f_name = 'WC_EMP_Loc.csv'
    fl = dict()
    
    for record in table:
        ##Attributes
        rec0 = record.rec [0:10]
        f = 0
        
        if rec0 in EmpNumList:
            fl[record.rec] = (record.loc + "," + record.classcode + "," + record.category.replace(","," ") + "," + record.numemps + "," + record.premium.replace(",","") +
                              "," + record.street.replace(","," ") + "," + record.city.replace(","," ") + "," + record.st + "," + record.zip)
            f += 1
    table.close()
       
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Reports")
    
    print ('Amount of WC policies: %d' %len(fl))
    
    with open(f_name , 'w') as f:
        f.write('Rec,Location,Classcode,Category,EmpNum,Premium,Street,City,ST,Zip\n')
        for key in fl.keys():
            f.write("%s,%s\n"%(key,fl[key]))
    f.close()
    
main()
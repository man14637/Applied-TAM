'''
Created on Sep 3, 2019

@author: michael
'''
import os
import dbf
import csv
from dbf import READ_ONLY

def main():
        #Declaring main variables CSR/Drive
    carrier = input("Please input CSV NAME: ")
    pol_list = []
    pol_csv(carrier,pol_list)
    pol(pol_list)
    

##reading csv of Company Codes and Names
def pol_csv(carrier,pol_list):
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    with open(carrier) as csv_file: 
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            rec1 = row[0]
            rec2 = rec1 [-7:]
            pol_list.append(rec2)
            line_count += 1
        print (line_count)
        
def pol(pol_list):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")

    table = dbf.Table("AP1DBDB.DBF")
    table.open(mode=READ_ONLY)
    table.use_deleted = False
    
    ##Declaring Variables
    f_name = 'pol_.csv'
    fl = dict()
    
    for record in table:
        ##Attributes
        rec0 = record.rec [0:10]
        f = 0
        
        if rec0 in pol_list:
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

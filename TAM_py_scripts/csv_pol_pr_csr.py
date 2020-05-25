'''
Created on Sep 29, 2019

@author: michael
'''
import os
import dbf
import csv

def main():
    #Declaring main variables CSR/Drive
    pol_csv = input("Input CSV NAME: ")
    cus_chg = {}
    
    co_csv(pol_csv,cus_chg)
    
    policy(cus_chg)
    
##reading csv of Company Codes and Names
def co_csv(pol_csv,cus_chg):
    os.chdir("D:\\TAM\\Data")
    with open(pol_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            cus_chg[row[0]]= {'CSR': row[1],'PR': row[2]}
            line_count += 1
        print (line_count)
        input("Press Enter to exit......")

def policy(cus_chg):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("D:\\TAM\\Data")
    
    table = dbf.Db3Table("POLICY.DBF")
    table.use_deleted = False
    table.open(mode=dbf.READ_WRITE)
    print ("Database Size: %d" % table.__len__())
    
    z = 0
    
    #Skip deleted records
    
    for record in dbf.Process(table):
        
        if record.pol_idx in cus_chg:
            z = z + 1
            record.csr = cus_chg[record.pol_idx]['CSR']
            record.pr = cus_chg[record.pol_idx]['PR']
        #End of if C for customer
    table.close()
    print("Number of Policy.dbf records Processed %d" % (z))
    input("Press Enter to exit......")
#Closing INS Database and DBF

main()

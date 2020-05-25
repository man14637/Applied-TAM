'''
Created on Aug 8, 2019

@author: michael
'''

import os
import dbf
import csv

def main():
    #Declaring main variables CSR/Drive
    cus_csr = input("Input CSV NAME: ")
    bco = {}
    
    co_csv(cus_csr,bco)
    ins(bco)
    policy(bco)
    
##reading csv of Company Codes and Names
def co_csv(cus_csr,bco):
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    with open(cus_csr) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            cus_csr[row[0]]= row[1]
            line_count += 1
        print (line_count)

def ins(csrOne,bco) :
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    
    table = dbf.Db3Table("INS.DBF")
    table.open(mode=dbf.READ_WRITE)
    table.use_deleted = False
    
    z = 0
    tamC = ''
    aa = ''
    c_ins = ''
    c_ins.encode('latin-1')
    csrOne.encode('latin-1')
    
    #Skip deleted records
    
    print ("INS Database Size: %d" % table.__len__())
    
    for record in dbf.Process(table):
        c_ins = record.c
        aa = c_ins[:2]
        
        if record.rec in bco:
            z = z + 1
            num = int(len(c_ins))
            aa = csrOne.upper()
            ab = c_ins[2:num]
            tamC = aa + ab
            tamC.encode('latin-1')
            record.c = tamC
        #End of if C for customer
    table.close()
    print("Number of INS.dbf records Processed %d" % (z))
#Closing INS Database and DBF

def policy(csrOne,bco):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")

    table = dbf.Db3Table("POLICY.DBF")
    table.use_deleted = False
    table.open(mode=dbf.READ_WRITE)
    print ("Database Size: %d" % table.__len__())
    
    z = 0
    csrOne.encode('latin-1')
    
    #Skip deleted records
    
    for record in dbf.Process(table):
        pol_split = record.pol_idx
        pol_split = pol_split[0:7]
        
        if pol_split in bco:
            z = z + 1
            record.csr = csrOne.upper()
        #End of if C for customer
    table.close()
    print("Number of Policy.dbf records Processed %d" % (z))
    input("Press Enter to exit......")
#Closing INS Database and DBF

main()
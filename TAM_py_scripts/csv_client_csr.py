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
    cus_list = {}
    
    co_csv(cus_csr,cus_list)
    ins(cus_list)
    policy(cus_list)
    
##reading csv of Company Codes and Names
def co_csv(cus_csr,cus_list):
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    with open(cus_csr) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            cus_list[row[0]]= row[1]
            line_count += 1
        print (line_count)

def ins(cus_list) :
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    
    table = dbf.Db3Table("INS.DBF")
    table.open(mode=dbf.READ_WRITE)
    table.use_deleted = False
    
    z = 0
    tamC = ''
    
    #Skip deleted records
    
    print ("INS Database Size: %d" % table.__len__())
    
    for record in dbf.Process(table):
        
        if record.rec in cus_list:
            c_ins = record.c
            z = z + 1
            
            num = int(len(c_ins))
            aa = cus_list[record.rec]
            ab = c_ins[2:num]
            tamC = aa + ab
            record.c = tamC
        #End of if C for customer
    table.close()
    print("Number of INS.dbf records Processed %d" % (z))
#Closing INS Database and DBF

def policy(cus_list):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    
    table = dbf.Db3Table("POLICY.DBF")
    table.use_deleted = False
    table.open(mode=dbf.READ_WRITE)
    print ("Database Size: %d" % table.__len__())
    
    z = 0
    
    #Skip deleted records
    
    for record in dbf.Process(table):
        pol_split = record.pol_idx
        pol_split = pol_split[0:7]
        
        if pol_split in cus_list:
            z = z + 1
            record.csr = cus_list[pol_split]
        #End of if C for customer
    table.close()
    print("Number of Policy.dbf records Processed %d" % (z))
    input("Press Enter to exit......")
#Closing INS Database and DBF

main()

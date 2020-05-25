'''
Created on Aug 8, 2019

@author: michael
'''

import os
import dbf
import csv

def main():
    #Declaring main variables CSR/Drive
    client_csv = input("Input CSV NAME: ")
    prOne = input("Please enter the PR Code you would like to reassign:")
    cl_code = []
    
    co_csv(client_csv,cl_code)
    ins(prOne,cl_code)
    policy(prOne,cl_code)
    
##reading csv of Company Codes and Names
def co_csv(client_csv,cl_code):
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data\\reassign")
    with open(client_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            cl_code.append(row[0])
            line_count += 1
        print (line_count)

def ins(prOne,cl_code) :
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    
    table = dbf.Db3Table("INS.DBF")
    table.open(mode=dbf.READ_WRITE)
    table.use_deleted = False
    
    z = 0
    tamC = ''
    aa = ''
    c_ins = ''
    
    #Skip deleted records
    
    print ("INS Database Size: %d" % table.__len__())
    
    for record in dbf.Process(table):
        c_ins = record.c
        aa = c_ins[208:211]
        
        if record.rec in cl_code:
            z = z + 1
            num = len(c_ins)
            aa = prOne.upper()
            ab = c_ins[0:208]
            ac = c_ins[211:num]
            tamC = ab + aa + ac
            record.c = tamC
        #End of if C for customer
    table.close()
    print("Number of INS.dbf records Processed %d" % (z))
#Closing INS Database and DBF

def policy(prOne,cl_code):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    
    table = dbf.Db3Table("POLICY.DBF")
    table.use_deleted = False
    table.open(mode=dbf.READ_WRITE)
    print ("Database Size: %d" % table.__len__())
    
    z = 0
    prOne.encode('latin-1')
    
    #Skip deleted records
    
    for record in dbf.Process(table):
        pol_split = record.pol_idx
        pol_split = pol_split[0:7]
        
        if pol_split in cl_code:
            z = z + 1
            record.pr = prOne.upper()
        #End of if C for customer
    table.close()
    print("Number of Policy.dbf records Processed %d" % (z))
    input("Press Enter to exit......")
#Closing INS Database and DBF

main()
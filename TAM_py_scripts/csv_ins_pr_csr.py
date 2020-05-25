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
    cl_code = {}
    
    co_csv(client_csv,cl_code)
    ins(cl_code)
    
##reading csv of Company Codes and Names
def co_csv(client_csv,cl_code):
    os.chdir("D:\\TAM\\Data")
    with open(client_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            cl_code[row[0]]= {'CSR': row[1],'PR': row[2]}
            line_count += 1
        print (line_count)
        input("Press Enter to exit......")

def ins(cl_code) :
    
    #Reading DBF file / Changing to DBF Directory
    os.chdir("D:\\TAM\\Data")
    
    table = dbf.Db3Table("INS.DBF")
    table.open(mode=dbf.READ_WRITE)
    #Skip deleted records
    table.use_deleted = False
    
    z = 0

    print ("INS Database Size: %d" % table.__len__())
    
    for record in dbf.Process(table):
        c_ins = record.c
        
        if record.rec in cl_code and record.key =='C' and record.agcy != 3:
            z = z + 1
            num = len(c_ins)
            aa = cl_code[record.rec]['PR']
            ad = cl_code[record.rec]['CSR']
            ab = c_ins[2:208]
            ac = c_ins[211:num]
            tamC = ad + ab + aa + ac
            record.c = tamC
        #End of if C for customer
    table.close()
    print("Number of INS.dbf records Processed %d" % (z))
#Closing INS Database and DBF

main()

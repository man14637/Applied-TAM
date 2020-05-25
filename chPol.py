'''
Created on Jun 26, 2019

@author: michael
'''
import os
import dbf

def main():
    #Declaring main variables CSR/Drive
    agcy = input("Changing Agency Codes to 6 & 7.Please enter number 6 or 7: ")
    
    ins(agcy)

def ins(agcy):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\OneDrive - GROSSLIGHT INSURANCE INC\\TAM")
    
    table = dbf.Db3Table("INS.DBF")
    table.open(mode=dbf.READ_WRITE)
    table.use_deleted = False
    
    z = 0
    clients = []
    print ("INS Database Size: %d" % table.__len__())
    
    for record in dbf.Process(table):
        if record.key is 'C':
            if record.agcy == agcy:
                z = z + 1
                clients.append(record.rec)
            #End of if C for customer
    table.close()
    print("Number of INS.dbf records Processed %d" % (z))
    input("Press Enter to continue to Policy......")
    policy(clients)
#Closing INS Database and DBF

def policy(clients):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\OneDrive - GROSSLIGHT INSURANCE INC\\TAM")
    
    table = dbf.Db3Table("POLICY.DBF")
    table.use_deleted = False
    table.open(mode=dbf.READ_WRITE)
    print ("Database Size: %d" % table.__len__())
    
    z = 0
    #Skip deleted records
    rec = ""
    
    for record in dbf.Process(table):
        rec = record.pol_idx
        rec = rec[:7]
        
        
        if rec in clients:

            if record.agcy == str(1):
                z = z+1
                record.agcy = str(6)
            elif record.agcy == str(5):
                z = z+1
                record.agcy = str(7)
            elif record.agcy == str(2):
                z = z+1
                record.agcy = str(6)
        #End of if C for customer
    table.close()
    print("Number of Policy.dbf records Processed %d" % (z))
    input("Press Enter to exit......")
#Closing INS Database and DBF

main()